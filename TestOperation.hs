module Main (main) where

import qualified Operation as O
import qualified DBTypes as D 
import qualified DBUtils as DU
import Data.Maybe
import Control.Concurrent.STM
import qualified Data.Map.Lazy as L 
import Data.Typeable
import Control.Exception
import Test.QuickCheck (quickCheck)
import qualified Test.QuickCheck.Monadic as M
import System.Exit

create_empty_db :: STM(TVar D.Database)
create_empty_db = newTVar $ D.Database L.empty

main :: IO ()
main = return ()

