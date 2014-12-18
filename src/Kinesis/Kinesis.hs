{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE MultiParamTypeClasses     #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE RankNTypes                #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeFamilies              #-}

module Kinesis.Kinesis where

-------------------------------------------------------------------------------
import           Aws
import           Aws.Aws
import           Aws.Core
import           Aws.General
import           Aws.Kinesis
import           Control.Applicative
import           Control.Concurrent
import           Control.Error
import           Control.Exception            (IOException)
import           Control.Lens
import           Control.Monad.Catch
import           Control.Monad.Morph
import           Control.Monad.Reader
import           Control.Monad.Trans.Resource
import           Control.Retry
import           Data.Conduit
import qualified Data.Conduit.List            as C
import           Data.Monoid
import           Data.String.Conv
import           Data.Text                    (Text)
import           Network.HTTP.Conduit
import           Network.HTTP.Types.Status
-------------------------------------------------------------------------------
import           Kinesis.Types
-------------------------------------------------------------------------------


-------------------------------------------------------------------------------
-- | Get all shards for this application.
getAllShards
    :: (MonadIO m, MonadReader AppEnv m, MonadCatch m,
        MonadBaseControl IO m)
    => EitherT String m [Shard]
getAllShards = bimapEitherT show id $ do
    nm <- getStream
    let ds = DescribeStream Nothing Nothing nm
    runResourceT (awsIteratedList' (runKinesis 10) ds $$ C.take 10000)


-------------------------------------------------------------------------------
-- | Produce an infinite stream of records from shard.
streamRecords
    :: (Functor n, MonadIO n, MonadReader AppEnv n, MonadCatch n)
    => ShardId
    -> Maybe SequenceNumber
    -> Maybe Int
    -> Producer (ResourceT n) Record
streamRecords sid sn lim = do
    nm <- either (error.toS) id <$> runEitherT getStream
    let pos = case sn of
          Nothing -> TrimHorizon
          Just _ -> AfterSequenceNumber
        gsi = GetShardIterator sid pos sn nm
    iter <- lift $ getShardIteratorResShardIterator <$> runKinesis 10 gsi
    go (GetRecords lim iter)
  where

    go r = do
      a <- lift $ runKinesis 10 r
      case nextIteratedRequest r a of
        Nothing -> return ()
        Just r' -> do
          case null (getRecordsResRecords a) of
            True -> liftIO $ threadDelay 1000000
            False -> C.sourceList (getRecordsResRecords a)
          go r'


-------------------------------------------------------------------------------
-- | Grab stream name from app env.
getStream
    :: (Functor m, MonadReader AppEnv m) =>
     EitherT Text m StreamName
getStream = EitherT $ streamName <$> view appStream


-------------------------------------------------------------------------------
runKinesis
    :: (Transaction r b, MonadIO n, MonadReader AppEnv n, MonadCatch n,
        ServiceConfiguration r ~ KinesisConfiguration)
    => Int
    -> r
    -> ResourceT n b
runKinesis n r = runAws kc n r
    where
      kc = KinesisConfiguration UsEast1


-------------------------------------------------------------------------------
runAws
    :: (Transaction r b, MonadIO n,
        MonadReader AppEnv n, MonadCatch n)
    => ServiceConfiguration r NormalQuery
    -> Int
    -- ^ Number of retries
    -> r
    -- ^ Request
    -> ResourceT n b
runAws servConf n r = do
    mgr <- view appManager
    conf <- view appAwsConfig
    recovering (awsPolicy n) [kinesisH, httpRetryH, networkRetryH] $
      hoist liftIO $
      pureAws conf servConf mgr r


-------------------------------------------------------------------------------
kinesisH :: MonadIO m => Int -> Handler m Bool
kinesisH n = Handler $ \e -> do
  let chk = case e of
          KinesisErrorResponse cd _msg -> case cd of
            "ProvisionedThroughputExceededException" -> True
            _ -> False
          KinesisOtherError stat _ -> case stat of
            Status _ "Internal Server Error" -> True
            _ -> False
          _ -> False
  when chk $ retryMsg n e
  return chk


-------------------------------------------------------------------------------
awsPolicy :: Int -> RetryPolicy
awsPolicy n = capDelay 60000000 $
              mempty <> limitRetries n <> exponentialBackoff 25000



-------------------------------------------------------------------------------
-- | Which exceptions should we retry?
httpRetryH :: MonadIO m => Int -> Handler m Bool
httpRetryH n = Handler f
    where
      f e = do
          let chk = httpRetry e
          when chk $ retryMsg n e
          return chk


-------------------------------------------------------------------------------
-- | Should given exception be retried?
httpRetry :: HttpException -> Bool
httpRetry e =
    case e of
      TooManyRetries{} -> True
      ResponseTimeout{} -> True
      FailedConnectionException{} -> True
      FailedConnectionException2{} -> True
      InternalIOException{} -> True
      -- ProxyConnectException{} -> True
      StatusCodeException{} -> True
      NoResponseDataReceived{} -> True
      ResponseBodyTooShort{} -> True
      InvalidChunkHeaders{} -> True
      IncompleteHeaders{} -> True
      _ -> False


-------------------------------------------------------------------------------
-- | 'IOException's should be retried
networkRetryH :: MonadIO m => Int -> Handler m Bool
networkRetryH n = Handler $ \ (e :: IOException) -> do
    retryMsg n e
    return True


-------------------------------------------------------------------------------
retryMsg :: (Show e, MonadIO m) => Int -> e -> m ()
retryMsg n e = liftIO $ putStrLn $
  "[retry:" <> show n <> "] Encountered " <> show e <> ". Retrying."



