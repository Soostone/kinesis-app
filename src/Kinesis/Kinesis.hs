{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE MultiParamTypeClasses     #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE RankNTypes                #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeFamilies              #-}

module Kinesis.Kinesis where

-------------------------------------------------------------------------------
import           Aws
import           Aws.Aws
import           Aws.General
import           Aws.Kinesis
import           Control.Applicative
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
import           Data.Default
import           Data.String.Conv
import           Network.HTTP.Conduit
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
    nm <- EitherT $ streamName <$> view (appName . unAppName)
    let ds = DescribeStream Nothing Nothing nm
    runResourceT (awsIteratedList' (runKinesis 10) ds $$ C.take 1000)


-------------------------------------------------------------------------------
-- | Produce an infinite stream of records from shard.
streamRecords
    :: (Functor n, MonadIO n, MonadReader AppEnv n, MonadCatch n)
    => ShardId
    -> Producer (ResourceT n) Record
streamRecords sid = do
    nm <- either (error.toS) id . streamName <$> view (appName . unAppName)
    let gsi = GetShardIterator sid AtSequenceNumber Nothing nm
    iter <- lift $ getShardIteratorResShardIterator <$> runKinesis 10 gsi
    let gr = GetRecords Nothing iter
    awsIteratedList' (runKinesis 10) gr



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
    recovering (awsPolicy n) [httpRetryH, networkRetryH] $
      hoist liftIO $
      pureAws conf servConf mgr r



-------------------------------------------------------------------------------
awsPolicy :: Int -> RetryPolicy
awsPolicy n = capDelay 60000000 $
              def <> limitRetries n <> exponentialBackoff 25000



-------------------------------------------------------------------------------
-- | Which exceptions should we retry?
httpRetryH :: Monad m => Int -> Handler m Bool
httpRetryH _ = Handler f
    where
      f = return . httpRetry


-------------------------------------------------------------------------------
-- | Should given exception be retried?
httpRetry :: HttpException -> Bool
httpRetry e =
    case e of
      TooManyRetries{} -> True
      ResponseTimeout{} -> True
      FailedConnectionException{} -> True
      InternalIOException{} -> True
      -- ProxyConnectException{} -> True
      StatusCodeException{} -> True
      _ -> False


-------------------------------------------------------------------------------
-- | 'IOException's should be retried
networkRetryH :: Monad m => Int -> Handler m Bool
networkRetryH = const $ Handler $ \ (_ :: IOException) -> return True


