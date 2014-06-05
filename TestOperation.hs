module Main (main) where

import qualified Operation as O
import qualified DBTypes as D 
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
main = do quickCheck can_create_table

can_create_table tablename field_and_default = M.monadicIO test
    where test = do res <- M.run $ atomically $ do db <- create_empty_db
                                                   _ <- O.create_table db (D.TransactionID "blah" 0) tablename (fmap (\(f, d)-> (f, Just(D.Element(Just d)), typeOf(d::Int))) field_and_default) Nothing
                                                   O.show_tables db
                    let D.Tablename str = tablename
                    M.assert (res == "dasd")