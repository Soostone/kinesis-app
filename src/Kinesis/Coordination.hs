{-# LANGUAGE BangPatterns              #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE MultiParamTypeClasses     #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}

module Kinesis.Coordination where

-------------------------------------------------------------------------------
import           Aws.Kinesis
import           Control.Arrow
import           Control.Error
import           Control.Lens                hiding (assign)
import           Control.Monad
import           Control.Monad.Catch
import           Control.Monad.Reader

import           Control.Monad.Trans.Control
import           Data.List
import           Data.Map.Strict             (Map)
import qualified Data.Map.Strict             as M
import           Data.Ord
import qualified Data.Set                    as S
import           Data.Time
-------------------------------------------------------------------------------
import           Kinesis.Kinesis
import           Kinesis.Redis
import           Kinesis.Types
-------------------------------------------------------------------------------



-------------------------------------------------------------------------------
-- | Checkpoint node state.
checkpointNode ns = undefined


-------------------------------------------------------------------------------
-- | Initialize self as a new node in the cluster.
introduceNode = undefined



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
-- | Do a complete pass over shards and node information.
balanceCluster
    :: (MonadIO m, MonadReader AppEnv m, MonadCatch m, MonadBaseControl IO m)
    => EitherT String m ()
balanceCluster = do
    shards <- getAllShards
    nodes <- getAllNodes
    states <- getAllShardStates

    now <- liftIO $ getCurrentTime

    let (deadNodes, aliveNodes) = partition (isDeadNode now) nodes

        curAssignment = collect _shardNode (return . _shardId) (++) states

        newAssignment = assign (map _nodeId aliveNodes) (map shardShardId shards) curAssignment

        newAssignment' = invertMap newAssignment

        oldStateIx = M.fromList $ map (_shardId &&& id) states


    -- remove nodes that have disappeared
    delNodes (map _nodeId deadNodes)


    -- remove shards that have been deleted
    forM_ states $ \ ss ->
      whenJust (M.lookup (ss ^. shardId) newAssignment') $ const $ void $
        delShardStates [ss ^. shardId]


    -- update shard assignments on database
    forM_ (M.toList newAssignment') $ \ (sid, nid) -> case M.lookup sid oldStateIx of
      Just ss -> setShardState (ss & shardNode .~ nid)
      Nothing -> setShardState (ShardState sid nid Nothing now 0)


-------------------------------------------------------------------------------
-- | Assess if a node is dead
isDeadNode :: UTCTime -> Node -> Bool
isDeadNode now n = diffUTCTime now (n ^. nodeLastBeat) < 120



-------------------------------------------------------------------------------
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
