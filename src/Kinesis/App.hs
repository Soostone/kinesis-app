{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE MultiParamTypeClasses     #-}
{-# LANGUAGE NoMonomorphismRestriction #-}

module Kinesis.App where

-------------------------------------------------------------------------------
import           Aws.Kinesis
import           Control.Lens
import           Control.Monad.Catch
import           Control.Monad.Reader
import           Control.Retry
import qualified Data.ByteString.Char8 as B
import           Data.Monoid
import           Database.Redis
import           Database.Redis.Utils
-------------------------------------------------------------------------------
import           Kinesis.Types
-------------------------------------------------------------------------------


-------------------------------------------------------------------------------
dictKey :: MonadReader AppEnv m => B.ByteString -> m B.ByteString
dictKey suffix = do
    nm <- view $ appName . unAppName
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
-- | Perform an action while acquiring configuration lock.
lockingConfig
    :: (MonadIO m, MonadReader AppEnv m, MonadMask m)
    => m b
    -> m b
lockingConfig f = do
    lk <- lockKey
    r <- view appRedis

    let lock lk = liftIO $ runRedis r $ blockLock policy "kinesis" 10 lk
        unlock lk = liftIO $ runRedis r $ releaseLock "kinesis" lk

    bracket_ (lock lk) (unlock lk) f

  where

    policy = capDelay 10000000 $
             exponentialBackoff 25000 <> limitRetries 10



