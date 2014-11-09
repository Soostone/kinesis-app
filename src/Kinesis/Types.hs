{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TemplateHaskell            #-}

module Kinesis.Types where

-------------------------------------------------------------------------------
import           Aws
import           Aws.Kinesis.Types
import           Control.Concurrent.Async
import           Control.Concurrent.MVar
import           Control.Lens
import           Data.Aeson
import           Data.Aeson.TH
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
    , _appIp        :: Text
    , _appNodeId    :: NodeId
    , _appConfig    :: AppConfig
    }


-------------------------------------------------------------------------------
data AppConfig = AppConfig {
      _configLoopDelay  :: Int
      -- ^ How often we checkpoint node state (microseconds)
    , _configGraceDelay :: NominalDiffTime
    -- ^ How long before we start working on a new assignment
    , _configNodeBeat   :: NominalDiffTime
    -- ^ How long before we consider a node dead
    }


-------------------------------------------------------------------------------
-- | In-memory state of a node
data NodeState = NodeState {
      _nsWorkers :: ! (Map ShardId (MVar Worker, Async ()))
    }


-------------------------------------------------------------------------------
-- | Last state for each known shard in cluster.
data ShardState = ShardState {
      _shardId       :: ShardId
    , _shardNode     :: NodeId
    , _shardSeq      :: Maybe SequenceNumber
    , _shardLastBeat :: UTCTime
    , _shardAssigned :: UTCTime
    -- ^ When the assignment was made. We wait a grace period after
    -- this to start processing in case assignments change rapidly.
    , _shardItems    :: !Int64
    } deriving (Eq,Show,Read,Ord)


-------------------------------------------------------------------------------
-- | Node metadata stored in database
data Node = Node {
      _nodeId       :: NodeId
    , _nodeIp       :: Text
    , _nodeLastBeat :: UTCTime
    , _nodeInit     :: UTCTime
    } deriving (Eq,Show,Read,Ord)


-------------------------------------------------------------------------------
-- | In-memory worker state.
data Worker = Worker {
      _workerId            :: ! WorkerId
    , _workerShard         :: ! ShardId
    , _workerLastBeat      :: ! UTCTime
    , _workerLastProcessed :: ! (Maybe SequenceNumber)
    , _workerItems         :: ! Int64
    } deriving (Eq,Show,Read,Ord)



-------------------------------------------------------------------------------
makeLenses ''WorkerId
makeLenses ''NodeId
makeLenses ''AppName
makeLenses ''Worker
makeLenses ''Node
makeLenses ''AppEnv
makeLenses ''AppConfig
makeLenses ''NodeState
makeLenses ''ShardState
-------------------------------------------------------------------------------


-------------------------------------------------------------------------------
$(deriveJSON defaultOptions {fieldLabelModifier = drop 7, constructorTagModifier = map toLower} ''Worker)
$(deriveJSON defaultOptions {fieldLabelModifier = drop 6, constructorTagModifier = map toLower} ''ShardState)
$(deriveJSON defaultOptions {fieldLabelModifier = drop 5, constructorTagModifier = map toLower} ''Node)
-------------------------------------------------------------------------------


