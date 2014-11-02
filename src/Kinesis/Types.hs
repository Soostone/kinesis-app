{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TemplateHaskell            #-}

module Kinesis.Types where

-------------------------------------------------------------------------------
import           Aws
import           Aws.Kinesis.Types
import           Control.Concurrent.Async
import           Control.Lens
import           Data.Aeson
import           Data.Aeson.TH
import           Data.ByteString.Char8    (ByteString)
import           Data.Char
import           Data.Int
import           Data.Map.Strict          (Map)
import           Data.Text                (Text)
import           Data.Time
import qualified Database.Redis           as R
import           Network.HTTP.Client
-------------------------------------------------------------------------------


-------------------------------------------------------------------------------
newtype AppName = AppName { _unAppName :: Text }
  deriving (Eq,Show,Read,Ord,ToJSON,FromJSON)


-------------------------------------------------------------------------------
newtype WorkerId = WorkerId { _unWorkerId :: Text }
  deriving (Eq,Show,Read,Ord,ToJSON,FromJSON)


-------------------------------------------------------------------------------
newtype NodeId = NodeId { _unNodeId :: Text }
  deriving (Eq,Show,Read,Ord,ToJSON,FromJSON)


-------------------------------------------------------------------------------
data AppEnv = AppEnv {
      _appName      :: AppName
    , _appRedis     :: R.Connection
    , _appManager   :: Manager
    , _appAwsConfig :: Aws.Configuration
    }


-------------------------------------------------------------------------------
data ShardState = ShardState {
      _shardId       :: ShardId
    , _shardNode     :: NodeId
    , _shardSeq      :: Maybe SequenceNumber
    , _shardLastBeat :: UTCTime
    , _shardItems    :: !Int64
    } deriving (Eq,Show,Read,Ord)


-------------------------------------------------------------------------------
-- | Node metadata stored in database
data Node = Node {
      _nodeId       :: NodeId
    , _nodeIp       :: Text
    , _nodeLastBeat :: UTCTime
    } deriving (Eq,Show,Read,Ord)


-------------------------------------------------------------------------------
-- | Worker metadata stored in database
data Worker = Worker {
      _workerId            :: WorkerId
    , _workerShard         :: ShardId
    , _workerLastBeat      :: UTCTime
    , _workerLastProcessed :: Maybe SequenceNumber
    , _workerItems         :: !Int64
    } deriving (Eq,Show,Read,Ord)


-------------------------------------------------------------------------------
-- | In-memory state of a node
data NodeState = NodeState {
      _nsNodeId  :: NodeId
    , _nsWorkers :: ! (Map ShardId (Worker, Async ()))
    }



-------------------------------------------------------------------------------
makeLenses ''WorkerId
makeLenses ''NodeId
makeLenses ''AppName
makeLenses ''Worker
makeLenses ''Node
makeLenses ''AppEnv
makeLenses ''NodeState
makeLenses ''ShardState
-------------------------------------------------------------------------------


-------------------------------------------------------------------------------
$(deriveJSON defaultOptions {fieldLabelModifier = drop 7, constructorTagModifier = map toLower} ''Worker)
$(deriveJSON defaultOptions {fieldLabelModifier = drop 6, constructorTagModifier = map toLower} ''ShardState)
$(deriveJSON defaultOptions {fieldLabelModifier = drop 5, constructorTagModifier = map toLower} ''Node)
-------------------------------------------------------------------------------


