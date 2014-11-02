module Main (main) where

-------------------------------------------------------------------------------
import           Test.Tasty
-------------------------------------------------------------------------------
import           Kinesis.CoordinationTests (coordinationTests)
-------------------------------------------------------------------------------

main = defaultMain $ testGroup "tests"
    [ coordinationTests
    ]
