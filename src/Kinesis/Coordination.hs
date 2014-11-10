{-# LANGUAGE BangPatterns              #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE MultiParamTypeClasses     #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}

module Kinesis.Coordination where

-------------------------------------------------------------------------------
import           Aws.Kinesis
import           Control.Arrow
import           Control.Concurrent           (threadDelay)
import           Control.Concurrent.Async
import           Control.Concurrent.MVar
import           Control.Error
import           Control.Exception.Base       (evaluate)
import           Control.Lens                 hiding (assign)
import           Control.Monad
import           Control.Monad.Catch
import           Control.Monad.Reader
import           Control.Monad.State.Strict
import           Control.Monad.Trans.Control
import           Control.Monad.Trans.Resource
import           Data.Conduit
import           Data.List
import           Data.Map.Strict              (Map)
import qualified Data.Map.Strict              as M
import           Data.Ord
import           Data.RNG
import qualified Data.Set                     as S
import           Data.String.Conv
import           Data.Time
-------------------------------------------------------------------------------
import           Kinesis.Kinesis
import           Kinesis.Redis
import           Kinesis.Types
-------------------------------------------------------------------------------


-------------------------------------------------------------------------------
-- | The single-threaded master control loop.
masterLoop
    :: (MonadIO m, MonadReader AppEnv m, MonadState NodeState m,
        MonadCatch m, MonadBaseControl IO m)
    => Sink Record (ResourceT (ReaderT AppEnv IO)) ()
    -> EitherT String m b
masterLoop f = do
    introduceNode

    forever $ do
      implementAssignments f
      checkpointNode
      liftIO $ threadDelay 30000000


-------------------------------------------------------------------------------
-- | Look into workers managed by this node and checkpoint their
-- status to central database.
checkpointNode
    :: (Functor m, MonadIO m, MonadReader AppEnv m,
        MonadState NodeState m)
    => EitherT String m ()
checkpointNode = do
    ws <- use $ nsWorkers . to M.toList
    forM_ ws $ \ (_sid, (mv, _a)) ->
      syncShardState =<< liftIO (readMVar mv)


-------------------------------------------------------------------------------
-- | Initialize self as a new node in the cluster.
introduceNode
    :: (MonadIO m, MonadReader AppEnv m, MonadCatch m,
        MonadBaseControl IO m)
    => EitherT String m ()
introduceNode = do
    mkNode >>= setNode
    balanceCluster


-------------------------------------------------------------------------------
mkNode :: (MonadIO m, MonadReader AppEnv m) => m Node
mkNode = do
    ip <- view appIp
    nid <- view appNodeId
    now <- liftIO getCurrentTime
    return $ Node nid ip now now


-------------------------------------------------------------------------------
-- | Query for assignments from Redis and implement this node's
-- responsibility by forking off worker processes.
implementAssignments
    :: (Functor m, MonadIO m, MonadReader AppEnv m,
        MonadState NodeState m)
    => Sink Record (ResourceT (ReaderT AppEnv IO)) ()
    -> EitherT String m ()
implementAssignments work = do
    nid <- view appNodeId

    allStates <- getAllShardStates
    let m = collectAssignments allStates

    states <- hoistEither $ note "Node has no assignment" $ M.lookup nid m
    let sids = map _shardId states

    current <- use nsWorkers

    let isOld k _ = not $ k `elem` sids

        isNew s = isNothing $ M.lookup (_shardId s) current

        kills = M.toList $ M.filterWithKey isOld current

        news = filter isNew states


    -- kill cancelled assignments
    forM_ kills $ \ (sid, (_w, a)) -> do
      liftIO $ cancel a
      nsWorkers . at sid .= Nothing


    -- implement new workers
    ae <- ask
    forM_ news $ \ s -> do
      wid <- mkWorkerId
      now <- liftIO getCurrentTime
      grace <- view $ appConfig . configGraceDelay

      when (diffUTCTime now (s ^. shardAssigned) > grace) $ do
        let sid = s ^. shardId
        w <- liftIO $ newMVar (Worker wid sid now Nothing 0)
        a <- liftIO $ async (runReaderT (runWorker s w work) ae)
        nsWorkers . at sid .= Just (w, a)


-------------------------------------------------------------------------------
mkWorkerId :: MonadIO m => m WorkerId
mkWorkerId = liftIO $ liftM (WorkerId . toS) $ mkRNG >>= randomToken 32


-------------------------------------------------------------------------------
runWorker
    :: (MonadIO m, MonadReader AppEnv m, MonadCatch m, MonadBaseControl IO m)
    => ShardState
    -> MVar Worker
    -> Sink Record (ResourceT m) a
    -> m a
runWorker s mw f = runResourceT $ streamRecords sid sn =$= updateWorker $$ f
    where
      sid = s ^. shardId
      sn = s ^. shardSeq

      updateWorker = awaitForever $ \ record -> do
        yield record
        liftIO $ modifyMVar_ mw $ \ w -> evaluate $ w
          & workerLastProcessed .~ Just (recordSequenceNumber record)
          & workerItems %~ (+1)



-------------------------------------------------------------------------------
-- | Sync state of shard from worker state in memory.
syncShardState
    :: (Functor m, MonadIO m, MonadReader AppEnv m)
    => Worker
    -> EitherT String m Bool
syncShardState w = do
    ss <- getShardState (w ^. workerShard)
    setShardState $ ss
      & shardSeq      .~ (w ^. workerLastProcessed)
      & shardLastBeat .~ (w ^. workerLastBeat)
      & shardItems    .~ (w ^. workerItems)



-------------------------------------------------------------------------------
-- | Build index of shard states by node id.
collectAssignments
    :: Traversable t
    => t ShardState
    -> Map NodeId [ShardState]
collectAssignments states = collect _shardNode return (++) states


-------------------------------------------------------------------------------
buildStateIx :: [ShardState] -> Map ShardId ShardState
buildStateIx states = M.fromList $ map (_shardId &&& id) states


-------------------------------------------------------------------------------
-- | Do a complete pass over shards and node information.
balanceCluster
    :: (MonadIO m, MonadReader AppEnv m, MonadCatch m, MonadBaseControl IO m)
    => EitherT String m ()
balanceCluster = do
    shards <- getAllShards
    nodes <- getAllNodes
    states <- getAllShardStates

    now <- liftIO $ getCurrentTime
    nodeDeadGrace <- view $ appConfig . configNodeBeat

    let isDeadNode n = diffUTCTime now (n ^. nodeLastBeat) < nodeDeadGrace

        (deadNodes, aliveNodes) = partition isDeadNode nodes

        curAssignment = M.map (map _shardId) $ collectAssignments states

        newAssignment = assign (map _nodeId aliveNodes) (map shardShardId shards) curAssignment

        newAssignment' = invertMap newAssignment

        oldStateIx = buildStateIx states


    -- remove nodes that have disappeared
    delNodes (map _nodeId deadNodes)


    -- remove shards that have been deleted
    forM_ states $ \ ss ->
      whenJust (M.lookup (ss ^. shardId) newAssignment') $ const $ void $
        delShardStates [ss ^. shardId]


    -- update shard assignments on database
    forM_ (M.toList newAssignment') $ \ (sid, nid) -> case M.lookup sid oldStateIx of
      Just ss -> setShardState (ss & shardNode .~ nid)
      Nothing -> setShardState (ShardState sid nid Nothing now now 0)



-------------------------------------------------------------------------------
-- | Assign a bunch of bs to as evenly, while minimally disturbing
-- their apriori assignment.
assign :: (Eq a, Eq b, Ord a, Ord b) => [a] -> [b] -> Map a [b] -> Map a [b]
assign as bs cur = new
    where
      cur' = M.toList cur

      oldAs = map fst cur'
      deadAs = S.fromList $ oldAs \\ as
      newAs = as \\ oldAs

      oldBs = concatMap snd cur'
      deadBs = S.fromList $ oldBs \\ bs
      newBs = bs \\ oldBs

      new = M.fromList .

            -- finally, balance the assignments
            balanceAssignments .

            -- add all the new bs to the first a you see
            (ix 0 . _2 %~ (newBs ++ )) .

            -- add the brand new as with empty assignments
            (zip newAs (repeat []) ++ ) .

            -- remove dead bs
            (traverse . _2 %~ filter (not . (`S.member` deadBs))) .

            -- remove dead as
            filter (not . (`S.member` deadAs) . fst) $

            cur'


-------------------------------------------------------------------------------
balanceAssignments :: Ord b => [(a, [b])] -> [(a, [b])]
balanceAssignments as = go as'
    where
      as' = as & traverse . _2 %~ annotate
      annotate ls = (ls, length ls)


      go xs = case mx - mn < 2 of
                True -> xs & traverse . _2 %~ fst
                False -> go xs''
        where
          mx = fromMaybe 0 $! maximumOf (traverse . _2 . _2) xs
          mn = fromMaybe 0 $! minimumOf (traverse . _2 . _2) xs

          xs' = sortBy (comparing (snd . snd)) xs

          xs'' = xs' &
            over (_last . _2) (\ (!ls, !len) -> (tail ls, len - 1)) .
            over (_head . _2) (\ (!ls, !len) -> (el : ls, len + 1))

          el = head . fst . snd . last $ xs'


-------------------------------------------------------------------------------
-- | Build reverse index from index.
invertMap :: Ord k => Map a [k] -> Map k a
invertMap = M.fromList . concatMap (\ (k, vs) -> map (\ v -> (v, k)) vs) . M.toList


-------------------------------------------------------------------------------
collect :: (Ord k, Traversable t)
        => (a -> k)
        -- ^ key in collected map
        -> (a -> v)
        -- ^ value in collected map
        -> (v -> v -> v)
        -- ^ collapse function for value
        -> t a
        -- ^ something I can traverse, e.g. [a]
        -> M.Map k v
collect k v f as = foldrOf folded step M.empty as
    where
      step a r = M.insertWith f (k a) (v a) r




-------------------------------------------------------------------------------
whenJust :: Monad m => Maybe t -> (t -> m ()) -> m ()
whenJust (Just a) f = f a
whenJust Nothing _ = return ()
