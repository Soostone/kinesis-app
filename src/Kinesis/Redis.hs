{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE MultiParamTypeClasses     #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}

module Kinesis.Redis where

-------------------------------------------------------------------------------
import           Aws.Kinesis
import           Control.Error
import           Control.Lens
import           Control.Monad.Catch
import           Control.Monad.Reader
import           Control.Retry
import           Data.Aeson
import qualified Data.ByteString.Char8 as B
import           Data.String.Conv
import           Data.Text.Encoding    (encodeUtf8)
import           Database.Redis
import           Database.Redis.Utils
-------------------------------------------------------------------------------
import           Kinesis.Types
-------------------------------------------------------------------------------


-------------------------------------------------------------------------------
dictKey :: MonadReader AppEnv m => B.ByteString -> m B.ByteString
dictKey suffix = do
    nm <- view $ appName . unAppName . to encodeUtf8
    return $ "_kinesis_" <> nm <> "_" <> suffix


-------------------------------------------------------------------------------
nodeDictKey :: MonadReader AppEnv m => m B.ByteString
nodeDictKey = dictKey "nodes"


-------------------------------------------------------------------------------
workerDictKey :: MonadReader AppEnv m => m B.ByteString
workerDictKey = dictKey "workers"


-------------------------------------------------------------------------------
-- | Name of configuration lock key on redis.
lockKey :: MonadReader AppEnv m => m B.ByteString
lockKey = dictKey "lock"


-------------------------------------------------------------------------------
-- | Key where we keep dictionary of shard states.
shardKey :: MonadReader AppEnv m => m B.ByteString
shardKey = dictKey "shards"




-------------------------------------------------------------------------------
-- | ShardState Metadata
-------------------------------------------------------------------------------



-------------------------------------------------------------------------------
setShardState
    :: (MonadIO m, Functor m, MonadReader AppEnv m)
    => ShardState
    -> EitherT String m Bool
setShardState = setObject shardKey (shardId . to (toS.show))


-------------------------------------------------------------------------------
getShardState
    :: (Functor m, MonadIO m, MonadReader AppEnv m)
    => ShardId
    -> EitherT String m ShardState
getShardState sid = getObject shardKey (toS (show sid))


-------------------------------------------------------------------------------
getAllShardStates
    :: (Functor m, MonadIO m, MonadReader AppEnv m)
    => EitherT String m [ShardState]
getAllShardStates = getAllObjects shardKey


delShardStates
    :: (Functor m, MonadIO m, MonadReader AppEnv m)
    => [ShardId]
    -> EitherT String m Integer
delShardStates ids = delObject shardKey $ map (toS.show) ids


-------------------------------------------------------------------------------
-- | Node Metadata
-------------------------------------------------------------------------------


-------------------------------------------------------------------------------
setNode
    :: (Functor m, MonadIO m, MonadReader AppEnv m)
    => Node
    -> EitherT String m Bool
setNode = setObject nodeDictKey (nodeId . to (toS . show))


-------------------------------------------------------------------------------
getNode
    :: (Functor m, MonadIO m, MonadReader AppEnv m)
    => NodeId
    -> EitherT String m Node
getNode nid = getObject nodeDictKey (toS (show nid))


-------------------------------------------------------------------------------
delNodes
    :: (Functor m, MonadIO m, MonadReader AppEnv m)
    => [NodeId]
    -> EitherT String m Integer
delNodes nids = delObject nodeDictKey $ map (toS.show) nids


-------------------------------------------------------------------------------
getAllNodes
    :: (Functor m, MonadIO m, MonadReader AppEnv m)
    => EitherT String m [Node]
getAllNodes = getAllObjects nodeDictKey



-------------------------------------------------------------------------------
-- | Dictionary Object Operations
-------------------------------------------------------------------------------


-------------------------------------------------------------------------------
setObject getKey keyLens obj = do
    k <- lift getKey
    let sid = obj ^. keyLens
        pl  = toS $ encode obj
    runRedis' (hset k sid pl)


delObject getKey objKey = do
    k <- getKey
    runRedis' (hdel k objKey)


-------------------------------------------------------------------------------
getObject getKey objKey = do
    k <- lift getKey
    blob <-
      (runRedis' $ hget k objKey) >>=
      hoistEither .
      note ("No object in database for: (" <>  show objKey <> ", " <> show k <> ")")
    hoistEither $ eitherDecode' $ toS blob


-------------------------------------------------------------------------------
getAllObjects getKey = do
    k <- getKey
    ns <- runRedis' (hgetall k)
    forM ns $ \ (_,v) -> hoistEither $ eitherDecodeStrict' v




-------------------------------------------------------------------------------
-- |
-------------------------------------------------------------------------------


-------------------------------------------------------------------------------
-- | Perform an action while acquiring configuration lock.
lockingConfig
    :: (MonadIO m, MonadReader AppEnv m, MonadMask m)
    => m b
    -> m b
lockingConfig f = do
    lk <- lockKey
    r <- view appRedis

    let lock = liftIO $ runRedis r $ blockLock redisPolicy "kinesis" 10 lk
        unlock = liftIO $ runRedis r $ releaseLock "kinesis" lk

    bracket_ lock unlock f


-------------------------------------------------------------------------------
runRedis' f = do
    r <- view appRedis
    showError . EitherT $ liftIO $ recoverAll redisPolicy $ runRedis r f


-------------------------------------------------------------------------------
redisPolicy :: RetryPolicy
redisPolicy = capDelay 10000000 $ exponentialBackoff 25000 <> limitRetries 10


showError :: (Functor m, Show e) => EitherT e m b -> EitherT String m b
showError = bimapEitherT show id


