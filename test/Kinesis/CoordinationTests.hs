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
      withItems (\ as bs -> M.keys (assign as bs M.empty) == as)

  , testProperty "all bs are there" $
      withItems (\ as bs -> (sort . concat . M.elems $ (assign as bs M.empty)) == bs)

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

  , testProperty "changing as are handled properly" prop_changingAs

  ]


prop_changingAs :: NonEmpty Int -> NonEmpty Int -> NonEmpty Char -> Property IO
prop_changingAs as as2 bs  = forAll $
  let as' = clr as
      as2' = clr as2
      bs' = clr bs
      m = assign as' bs' M.empty
      m' = assign as2' bs' m
  in do
    withReason (M.keys m' == as2') "From keys not equal"
    withReason ((sort . concat) (M.elems m') == bs') "To keys not equal"


withReason :: Bool -> String -> Either String String
withReason test msg = if test then Right "OK" else Left msg


-------------------------------------------------------------------------------
withItems
    :: Testable IO a
    => ([Int] -> [Char] -> a)
    -> NonEmpty Int
    -> NonEmpty Char
    -> Property IO
withItems f as bs = forAll $ f as' bs'
  where
    as' = clr as
    bs' = clr bs


clr :: (Ord t, Eq t) => NonEmpty t -> [t]
clr = sort . nub . getNonEmpty






