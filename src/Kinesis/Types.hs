{-# LANGUAGE TemplateHaskell #-}

module Kinesis.Types where

-------------------------------------------------------------------------------
import           Aws.Kinesis.Types
import           Control.Lens
import           Data.ByteString.Char8 (ByteString)
import           Data.Int
import           Data.Time
import qualified Database.Redis        as R
import           Network.HTTP.Client
-------------------------------------------------------------------------------


-------------------------------------------------------------------------------
newtype AppName = AppName { _unAppName :: ByteString }
  deriving (Eq,Show,Read,Ord)


-------------------------------------------------------------------------------
newtype WorkerId = WorkerId { _unWorkerId :: ByteString }
  deriving (Eq,Show,Read,Ord)


-------------------------------------------------------------------------------
newtype NodeId = NodeId { _unNodeId :: ByteString }
  deriving (Eq,Show,Read,Ord)


-------------------------------------------------------------------------------
data AppEnv = AppEnv {
      _appName    :: AppName
    , _appRedis   :: R.Connection
    , _appManager :: Manager
    }


-------------------------------------------------------------------------------
data Node = Node {
      _nodeId      :: NodeId
    , _nodeIp      :: ByteString
    , _nodeWorkers :: [WorkerId]
    } deriving (Eq,Show,Read,Ord)


-------------------------------------------------------------------------------
data Worker = Worker {
      _workerId            :: WorkerId
    , _workerShard         :: ShardId
    , _workerLastBeat      :: UTCTime
    , _workerLastProcessed :: Maybe SequenceNumber
    , _workerLastProcAt    :: Maybe UTCTime
    , _workerItems         :: Int64
    } deriving (Eq,Show,Read,Ord)



-------------------------------------------------------------------------------
makeLenses ''WorkerId
makeLenses ''NodeId
makeLenses ''AppName
makeLenses ''Worker
makeLenses ''Node
makeLenses ''AppEnv
-------------------------------------------------------------------------------


