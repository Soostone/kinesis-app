{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TemplateHaskell       #-}

module Kinesis.CoordinationTests
    ( coordinationTests
    ) where

-------------------------------------------------------------------------------
import           Data.ByteString        (ByteString)
import           Data.ByteString.Char8  (unpack)
import           Data.List
import qualified Data.Map.Strict        as M
import           Data.Monoid
import           Test.SmallCheck
import           Test.SmallCheck.Series
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.SmallCheck
-------------------------------------------------------------------------------
import           Kinesis.Coordination
-------------------------------------------------------------------------------


coordinationTests = testGroup "Kinesis.Coordination"
  [ assignmentTests
  ]


assignmentTests :: TestTree
assignmentTests = testGroup "shard assignment"
  [ testProperty "all as are there" $
      withItems (\ as bs -> M.keys (assign as bs M.empty) == sort as)

  , testProperty "all bs are there" $
      withItems (\ as bs -> (sort . concat . M.elems $ (assign as bs M.empty)) == sort bs)

  , testProperty "assignment is balanced" $
      withItems (\ as bs ->
        let els = map length . M.elems $ assign as bs M.empty
            mn = minimum els
            mx = maximum els
        in mx - mn < 2 )

  , testProperty "repeated assignment is idempotent" $
      withItems (\ as bs ->
        let m = assign as bs M.empty
            m' = assign as bs m
        in m == m' )

  ]


-------------------------------------------------------------------------------
withItems
    :: Testable IO a
    => ([Int] -> [Char] -> a)
    -> NonEmpty Int
    -> NonEmpty Char
    -> Property IO
withItems f as bs = changeDepth (min 6) $ f as' bs'
  where
    clr :: Eq t => NonEmpty t -> [t]
    clr = nub . getNonEmpty
    as' = clr as
    bs' = clr bs






