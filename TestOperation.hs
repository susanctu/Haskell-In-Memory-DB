module Main (main) where

import Test.Hspec
import Test.QuickCheck
import qualified Operation as O
import qualified DBTypes as D 
import Data.Maybe
import Control.Concurrent.STM
import qualified Data.Map.Lazy as L 
import Data.Typeable
import Control.Exception
import qualified Test.QuickCheck.Monadic as M

create_empty_db :: STM(TVar D.Database)
create_empty_db = newTVar $ D.Database L.empty

main :: IO ()
main = do quickCheck can_create_table

can_create_table tablename field_and_default = monadicIO test
    where test = do res <- run $ atomically $ 
                   do db <- create_empty_db
	              _ <- O.create_table db (D.TransactionID "blah" 0) tablename (fmap (\(f, d)-> (f, Just(D.Element(Just d)), typeOf(d::Int))) field_and_default) Nothing
                      O.show_tables db
                   let Tablename str = tablename
                   assert $ res == str

  -- check that we can do table-level operations
  i{-describe "Create table" $ do
    it "Can create a table" $ M.monadicIO $ 
      \(tablename, field_and_default) -> atomically $ 
        do db <- create_empty_db
	   res <- O.create_table db (D.TransactionID "blah" 0) tablename (fmap (\(f, d)-> (f, Just(D.Element(Just d)), typeOf(d::Int))) field_and_default) Nothing
           case res of 
             Left _ -> M.assert (False) --test failed 
             Right _ -> return ()-}
    {-it "Can create detect duplicate table names or create two tables" $ property $ M.monadicIO $
      \(tablename1, tablename2, field_and_default) -> atomically $ 
        do db <- create_empty_db
	   res1 <- O.create_table db (D.TransactionID "blah" 0) tablename2 (fmap (\(f, d)-> (f, Just(D.Element(Just d)), typeOf(d::Int))) field_and_default) Nothing
           res2 <- O.create_table db (D.TransactionID "blah" 0) tablename2 (fmap (\(f, d)-> (f, Just(D.Element(Just d)), typeOf(d::Int))) field_and_default) Nothing
           if tablename1==tablename2
             then case res2 of 
               Left _ -> return () -- second op should have failed
               Right _ -> M.assert (False) --test failed  
             else return () 
    it "Can drop a table" $ property $ M.monadicIO $
      \(tablename, field_and_default) -> atomically $ 
        do db <- create_empty_db
	   _ <- O.create_table db (D.TransactionID "blah" 0) tablename (fmap (\(f, d)-> (f, Just(D.Element(Just d)), typeOf(d::Int))) field_and_default) Nothing
           str1 <- O.show_tables db
           M.assert (str1 /= "")
           _ <- O.drop_table db (D.TransactionID "blah" 1) tablename 
           str2 <- O.show_tables db
           M.assert (str2 == "") -}
    {- it "Can create a table and add some columns later" $ property $
      \(tablename, field_and_default) -> atomically $ 
        do db <- create_empty_db
    it "Can create a table and delete some columns later" $ property $
      \(tablename, field_and_default) -> atomically $ 
        do db <- create_empty_db
    it "Can create a table and add and delete some columns later" $ property $
      \(tablename, field_and_default) -> atomically $ 
        do db <- create_empty_db -}

  -- check that we insert values into a table and see them later using select

