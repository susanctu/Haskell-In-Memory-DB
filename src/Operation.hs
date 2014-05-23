{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ExistentialQuantification #-} 
{-# LANGUAGE RankNTypes #-}
module Operation (create_table, drop_table, alter_table_add, alter_table_drop, select, Operation.insert, Operation.update, Operation.delete, show_tables) where

import System.IO
import Control.Monad
import Control.Concurrent
import Control.Concurrent.STM
import Data.Map.Lazy

{- First stab at the interface -}
-- Note that this means that you'll have to keep track of the type 
-- for each table/col combo outside of this module since you need to 
-- parse the string into the correct haskell type before you attempt
-- inserts, for example. This means lookups on some Fieldname a should
-- never fail, but this module will handle the case when it does.

constructTableMap :: (forall a. [(Fieldname a, Maybe a)]) -> Map (Fieldname a) (Column a)
-> Map (Fieldname a) (Column a)
constructTableMap (Fieldname a, b):xs old_map = let col = Tvar empty 
  in constructTableMap xs (insert (Fieldname a) (Column {default_val=b, column=col})) 
constructTableMap _ old_map = old_map

create_table :: TVar Database -> Tablename -> (forall a. [(Fieldname a, Maybe a])) -> Maybe (Fieldname b) -> STM ((Either ErrString) LogOperation)
create_table db tablename field_and_col primary_key = do
  hmdb <- readTVar db
  -- TODO: first check that we've been given a valid primary key
  case lookup tablename (database hmdb) of 
    Just x -> do let table_map = constructTableMap field_and_col empty 
                 insert tablename Table{primaryKey=primary_key, table=table_map}  (database hmdb)
    Nothing -> return ErrString (show(tablename) ++ " already exists.")

drop_table :: TVar Database -> Tablename -> STM (Either (ErrString) LogOperation)
drop_table db tablename = do 
  hmdb <- readTVar db
  case lookup tablename (database hmdb) of 
    Just x -> do writeTvar db  (delete tablename (database hmdb))
                 return DropTable tablename  
    Nothing -> return ErrString (show(tablename) ++ " not found.")    

alter_table_add :: TVar Database -> String -> Fieldname a -> STM (Either (ErrString) LogOperation) 
alter_table_add db tablename fieldname =

alter_table_drop :: TVar Database -> String -> String -> STM (Either (ErrString) LogOperation)
alter_table_drop db tablename fieldname = 

-- String is readable into list of tuples 
select :: TVar Database -> (forall a. [(Fieldname a, (a -> Bool))]) -> Tablename -> STM(Either (ErrString) (LogOperation, IO String)) -- last string is the stuff user queried for
select db fieldnames_and_conds tablename =  

insert :: TVar Database -> Tablename -> (forall a. [(Fieldname a, a)]) -> STM(Either (ErrString) LogOperation) 
insert db tablename fieldnames_and_vals =

delete :: TVar Database -> Tablename -> (forall a. [(Fieldname a, (a -> Bool))]) -> STM(Either (ErrString) LogOperation)
delete db tablename filenames_and_conds =
 
update :: TVar Database -> Tablename -> (forall a. [(Fieldname a, a, (a -> Bool))]) -> STM (ErrString, LogOperation)
update db tablename filenames_vals_conds =
 
show_tables :: TVar Database -> STM (String) -- doesn't actually update the db, so no need for logstring
show_tables db =


