{-# LANGUAGE BangPatterns              #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE MultiParamTypeClasses     #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE RankNTypes                #-}
{-# LANGUAGE RecordWildCards           #-}
{-# LANGUAGE ScopedTypeVariables       #-}

module Kinesis.Coordination where

-------------------------------------------------------------------------------
import           Aws.Kinesis
import           Control.Arrow
import           Control.AutoUpdate
import           Control.Concurrent           (threadDelay)
import           Control.Concurrent.Async
import           Control.Concurrent.MVar
import           Control.Concurrent.STM
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
import qualified Data.Conduit.List            as C
import           Data.Default
import           Data.List
import           Data.Map.Strict              (Map)
import qualified Data.Map.Strict              as M
import           Data.Monoid
import           Data.Ord
import           Data.RNG
import qualified Data.Set                     as S
import           Data.String.Conv
import           Data.Time
import           LiveStats
import           Safe                         (fromJustNote)
import           System.IO
-------------------------------------------------------------------------------
import           Kinesis.Kinesis
import           Kinesis.Redis
import           Kinesis.Types
-------------------------------------------------------------------------------


-- | A processor is a record paired with an ACK action that you must
-- call upon completing record's processing.
type Processor  = Maybe (Record, IO ()) -> IO ()


-------------------------------------------------------------------------------
-- | Turn a sink into a processor
processorSink
    :: MonadIO n
    => Sink (Record, IO ()) n ()
    -> (forall a. n a -> IO a)
    -> Int
    -- ^ Concurrent queue buffer size
    -> IO Processor
processorSink f run lim = do
    ch <- atomically $ newTBQueue lim
    let go = do
            a <- liftIO $ atomically $ readTBQueue ch
            case a of
              Nothing -> return ()
              Just a' -> yield a' >> go

    -- exceptions in slave nodes kill the master process
    async (run $ go $$ f) >>= link
    return $ \ a -> atomically (writeTBQueue ch a)



-------------------------------------------------------------------------------
-- | The single-threaded master control loop.
masterLoop
    :: (MonadIO m, MonadCatch m, MonadBaseControl IO m, MonadMask m)
    => AppEnv
    -> IO Processor
    -> m (Either String ())
masterLoop ae f = flip evalStateT def . flip runReaderT ae  . runEitherT $ do
    liftIO $ hSetBuffering stdout LineBuffering
    delay <- view configLoopDelay <&> (*1000000)

    stats <- liftIO newStats
    liftIO $ addCounter stats "records"
    liftIO . async $ reportStats stats 30

    joinE $ lockingConfig introduceNode

    let controlLoop = forever $ do
          joinE $ lockingConfig $ do
            balanceCluster
            cs <- implementAssignments f stats
            checkpointWorkers
            when (ae ^. aeAppConfig . configVerbose) $
              echo (show cs)
            updateNode
          liftIO $ threadDelay delay

    controlLoop `catch`
      (\ (e :: SomeException) -> do
           echo $ "Encountered uncaught exception in masterLoop: " <> show e
           throwM e )


-------------------------------------------------------------------------------
-- | Look into workers managed by this node and checkpoint their
-- status to central database.
checkpointWorkers
    :: (Functor m, MonadIO m, MonadReader AppEnv m,
        MonadState NodeState m)
    => EitherT String m ()
checkpointWorkers = do
    echo "Checkpointing worker state..."
    ae <- ask
    ws <- use $ nsWorkers . to M.toList
    forM_ ws $ \ (_sid, (mv, _a)) -> do
      echo $ "Syncing state for " <> show _sid
      w <- liftIO $ readMVar mv
      when (ae ^. aeAppConfig . configVerbose) $
        echo $ "Worker state: " <> show w
      syncShardState w


-------------------------------------------------------------------------------
-- | Initialize self as a new node in the cluster.
introduceNode
    :: (MonadIO m, MonadReader AppEnv m, MonadCatch m,
        MonadBaseControl IO m)
    => EitherT String m ()
introduceNode = do
    echo "Introducing this node..."
    mkNode >>= setNode
    balanceCluster


-------------------------------------------------------------------------------
mkNode :: (MonadIO m, MonadReader AppEnv m) => m Node
mkNode = do
    nid <- view appNodeId
    now <- liftIO getCurrentTime
    return $ Node nid now now


-------------------------------------------------------------------------------
-- | Update node heartbeat.
updateNode
    :: (Functor m, MonadIO m, MonadReader AppEnv m)
    => EitherT String m Bool
updateNode = do
    nid <- view appNodeId
    n <- getNode nid
    now <- liftIO getCurrentTime
    setNode $ n & nodeLastBeat .~ now


-------------------------------------------------------------------------------
-- | Remove dead workers from state so they are re-spawned
pruneDeadWorkers :: (MonadIO m, MonadState NodeState m) => m ()
pruneDeadWorkers = do
    ws <- use $ nsWorkers . to M.toList
    forM_ ws $ \ (sid, (mv, a)) -> do
      chk <- liftIO $ poll a
      case chk of
        Nothing -> return ()
        Just (Right _) -> do
          w <- liftIO $ readMVar mv
          error $ "Unexpected: Worker " <> show (w ^. workerId ) <> " has finished working."
        Just (Left _) -> do
          w <- liftIO $ readMVar mv
          echo $ "Worker " <> show (w ^. workerId) <> " has died. Removing."
          nsWorkers . at sid .= Nothing


-------------------------------------------------------------------------------
-- | Query for assignments from Redis and implement this node's
-- responsibility by forking off worker processes.
implementAssignments
    :: ( Functor m, MonadIO m, MonadReader AppEnv m, MonadBaseControl IO m
       , MonadCatch m
       , MonadState NodeState m )
    => IO Processor
    -- ^ A processor maker
    -> Stats
    -> EitherT String m ClusterState
implementAssignments work stats = do
    echo "Implementing cluster assignments..."

    nid <- view appNodeId

    cs@ClusterState{..} <- getClusterState
    let m = collectAssignments _clusterActiveShards

    let states = fromMaybe [] $ M.lookup nid m
    let sids = map shardId states

    when (null states) $ echo $ "No shards have been assigned to this node..."

    pruneDeadWorkers

    current <- use nsWorkers

    now <- liftIO getCurrentTime
    grace <- view $ appConfig . configGraceDelay

    let isOld k _ = not $ k `elem` sids

        isNew s = isNothing $ M.lookup (shardId s) current

        kills = M.toList $ M.filterWithKey isOld current

        passedGrace s = diffUTCTime now (s ^. shardAssigned) > grace

        news = filter (\s -> isNew s && passedGrace s) states


    -- kill cancelled assignments
    when (not (null kills)) $
      echo $ "Killing " <> show (length kills) <> " workers..."

    forM_ kills $ \ (sid, (mv, a)) -> do
      w <- liftIO $ readMVar mv
      echo $ "Killing worker " <> show (w ^. workerId)
      liftIO $ cancel a
      nsWorkers . at sid .= Nothing


    -- implement new workers
    when (not (null news)) $
      echo $ "Spawning " <> show (length news) <> " new workers..."

    ae <- ask
    forM_ news $ \ s -> do
      wid <- mkWorkerId
      let sid = shardId s
          curSeq = _shardSeq s
          curCount = _shardItems s
      w <- liftIO $ newMVar (Worker wid sid now curSeq curCount)

      a <- liftIO $ async (runReaderT (runWorker stats s w work) ae)
      liftIO $ async $ do
        res <- waitCatch a
        case res of
          Right _ -> return ()
          Left e -> liftIO $ do
            w' <- readMVar w
            echo ("Worker died with "
                  <> show e <> ". " <> show w')

      nsWorkers . at sid .= Just (w, a)


    return cs

-------------------------------------------------------------------------------
mkWorkerId :: MonadIO m => m WorkerId
mkWorkerId = liftIO $ liftM (WorkerId . toS) $ mkRNG >>= randomToken 32


-------------------------------------------------------------------------------
runWorker
    :: (MonadIO m, MonadReader AppEnv m, MonadCatch m, MonadBaseControl IO m)
    => Stats
    -> ShardState
    -> MVar Worker
    -> IO Processor
    -> m ()
runWorker stats s mw mkProcessor = runResourceT $ do
    echo ("Initializing new worker for shard: " <> show s)

    mkNow <- liftIO $
      mkAutoUpdate defaultUpdateSettings { updateAction = getCurrentTime }

    liftIO $ async $ forever $ do
      threadDelay 1000000
      now <- mkNow
      modifyMVar_ mw $ \ w -> evaluate $ w
        & workerLastBeat .~ now

    c <- view configRecordBatch
    f <- liftIO mkProcessor
    streamRecords sid sn c =$= (go f) $$ C.sinkNull

    liftIO $ f Nothing

  where
    sid = shardId s
    sn = s ^. shardSeq

    go f = awaitForever $ \ record -> liftIO $ do
      let ack = do
              let !rseq = recordSequenceNumber record
                  pickLatest !a = maybe (Just rseq) (Just . (max rseq)) a
              modifyMVar_ mw $ \ w -> evaluate $ w
                & workerLastProcessed %~ pickLatest
                & workerItems %~ (+1)
              incStats stats "records" 1
      f (Just (record, ack))



-------------------------------------------------------------------------------
-- | Sync state of shard from worker state in memory.
syncShardState
    :: (Functor m, MonadIO m, MonadReader AppEnv m)
    => Worker
    -> EitherT String m Bool
syncShardState w = do
    ss <- getShardState (w ^. workerShard)
    now <- liftIO getCurrentTime

    let curSeq = w ^. workerLastProcessed
        completed = isJust curSeq &&
                    (curSeq == (ss ^. shard . to shardSequenceNumberRange . _2))

    setShardState $ ss
      & shardSeq       .~ curSeq
      & shardCompleted .~ completed
      & shardItems     .~ (w ^. workerItems)
      & shardLastBeat  .~ now



-------------------------------------------------------------------------------
-- | Build index of shard states by node id.
collectAssignments
    :: Traversable t
    => t ShardState
    -> Map NodeId [ShardState]
collectAssignments states = collect _shardNode return (++) states


-------------------------------------------------------------------------------
buildStateIx :: [ShardState] -> Map ShardId ShardState
buildStateIx states = M.fromList $ map (shardId &&& id) states



-------------------------------------------------------------------------------
getClusterState
    :: (MonadIO m, MonadReader AppEnv m, MonadCatch m, MonadBaseControl IO m)
    => EitherT String m ClusterState
getClusterState = do
    shards <- getAllShards
    states <- getAllShardStates
    let activeStates = filter (not . _shardCompleted) states

    (deadNodes, aliveNodes) <- getNodes

    let curAssign = invertMap $ M.map (map shardId) $ collectAssignments states
        newAssign = decideAssignments shards activeStates aliveNodes

    return $ ClusterState
      (M.fromList $ map (shardShardId &&& id) shards)
      states
      activeStates
      deadNodes
      aliveNodes
      curAssign
      newAssign
      (curAssign /= newAssign)


-------------------------------------------------------------------------------
-- | Get dead and alive nodes in cluster.
getNodes = do
    nodes <- getAllNodes
    now <- liftIO getCurrentTime
    nodeDeadGrace <- view $ appConfig . configNodeBeat
    let isDeadNode n = diffUTCTime now (n ^. nodeLastBeat) > nodeDeadGrace
    return $ partition isDeadNode nodes


-------------------------------------------------------------------------------
decideAssignments
    :: [Shard]
    -> [ShardState]
    -> [Node]
    -> Map ShardId NodeId
decideAssignments shards states aliveNodes = invertMap newAssignment
  where

    curAssignment :: Map NodeId [ShardId]
    curAssignment = M.map (map shardId) $ collectAssignments states

    newAssignment = assign
      (map _nodeId aliveNodes)
      (map shardShardId shards)
      curAssignment


-------------------------------------------------------------------------------
-- | Do a complete pass over shards and node information.
balanceCluster
    :: (MonadIO m, MonadReader AppEnv m, MonadCatch m, MonadBaseControl IO m)
    => EitherT String m ()
balanceCluster = do
    echo "Balancing cluster..."

    ClusterState{..} <- getClusterState
    let oldStateIx = buildStateIx _clusterShardStates


    -- first, remove nodes that have disappeared
    unless (null _clusterDeadNodes) $ void $ do
      echo $ "Deleting " <> show (length _clusterDeadNodes) <>
             " dead nodes: " <> show _clusterDeadNodes
      delNodes (map _nodeId _clusterDeadNodes)



    -- TODO remove shards that have been deleted
    -- forM_ _clusterShardStates $ \ ss ->
    --   whenJust (M.lookup (ss ^. shardId) _clusterNewAssignments) $ const $ void $ do
    --     echo $ "Deleting stale shard state: " <> show ss
    --     delShardStates [ss ^. shardId]

    now <- liftIO $ getCurrentTime


    -- update shard assignments on database
    forM_ (M.toList _clusterNewAssignments) $ \ (sid, nid) ->
      case M.lookup sid oldStateIx of
        Just ss -> case ss ^. shardNode == nid of
          True -> return ()
          False -> void $ setShardState (ss & shardNode .~ nid & shardAssigned .~ now)
        Nothing -> void $ setShardState $
          let s = fromJustNote "Impossible: ShardId not found" $ M.lookup sid _clusterShards
          in ShardState s nid Nothing False now now 0



-------------------------------------------------------------------------------
-- | Assign a bunch of bs to as evenly, while minimally disturbing
-- their apriori assignment.
assign :: (Eq a, Eq b, Ord a, Ord b) => [a] -> [b] -> Map a [b] -> Map a [b]
assign as bs cur = new
    where
      cur' = M.toList cur

      oldAs = map fst cur'
      deadAs' = oldAs \\ as
      deadAs = S.fromList deadAs'
      newAs = as \\ oldAs

      oldBs = concatMap snd cur'
      deadBs = S.fromList $ oldBs \\ bs
      newBs = bs \\ oldBs

      -- some Bs are going to be lost when we kill the As that carry
      -- them right now
      reassignBs = concat $ mapMaybe (flip M.lookup cur) deadAs'

      new = M.fromList .

            -- finally, balance the assignments
            balanceAssignments .

            -- add all the new bs to the first a you see
            (ix 0 . _2 %~ ((reassignBs ++ newBs) ++ )) .

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


