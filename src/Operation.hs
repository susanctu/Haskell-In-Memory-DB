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

constructTableMap :: (forall a. [(Fieldname a, Maybe a)]) -> Map (Fieldname a) (Column a) -> Map (Fieldname a) (Column a)
constructTableMap (Fieldname a, b):xs old_map = let col = Tvar empty 
  in constructTableMap xs (insert (Fieldname a) (Column {default_val=b, column=col})) 
constructTableMap _ old_map = old_map

create_table :: TVar Database -> Tablename -> (forall a. [(Fieldname a, Maybe a)]) -> Maybe (Fieldname b) -> STM ((Either ErrString) [LogOperation])
create_table db tablename field_and_col primary_key = do
  hmdb <- readTVar db
  if primary_key `elem` (map (\(x, y) -> x) field_and_col)
    then case lookup tablename (database hmdb) of 
           Just x -> do let table_map = constructTableMap field_and_col empty 
                        insert tablename Table{primaryKey=primary_key, table=table_map}  (database hmdb)
                        let op = CreateTable tablename
                        case primary_key of 
                          Nothing -> return [op] 
                          Just pk -> return [op, SetPrimaryKey Nothing primary_key tablename]
           Nothing -> return ErrString (show(tablename) ++ " already exists.")
    else return ErrString(show(primary_key) ++ "is not a valid primary key.") 

get_table :: TVar Database -> Tablename -> STM (Maybe (Tvar Table))
get_table db tablename =  do 
  hmdb <- readTVar db
  return lookup tablename (database hmdb)    

drop_table :: TVar Database -> Tablename -> STM (Either (ErrString) LogOperation)
drop_table db tablename = do 
  table <- get_table db tablename
  case table of 
    Just x -> do writeTvar db  (delete tablename (database hmdb))
                 return DropTable tablename  
    Nothing -> return ErrString (show(tablename) ++ " not found.")    


alter_table_add :: TVar Database -> String -> Fieldname a -> Maybe a -> Bool -> STM (Either (ErrString) [LogOperation]) 
alter_table_add db tablename fieldname default_val is_primary_key = do
  tvar_table <- get_table db tablename
  case tvar_table of 
    Just x -> do t <- readTVar x
                 case lookup fieldname (table t) of
                   Nothing -> do writeTvar x (insert fieldname Column{default_val=default_val,column=(Tvar empty)} t)
                                 let add_op = AddField fieldname tablename
                                 if is_primary_key
                                   then do let old_pk = primaryKey t
                                           modifyTvar x (set_primary_key t fieldname)
                                           return [add_op, SetPrimaryKey old_pk (Just fieldname) tablename]
                                   else return [add_op]
                   Just f -> return ErrString (show(f) ++ "already exists in " ++ show(tablename)) 
    Nothing -> return ErrString (show(tablename) ++ " not found.")    

set_primary_key :: Table -> Fieldname a -> Table
set_primary_key (Table pk t) f = Table {primary_key=(Just f), table=t} 

alter_table_drop :: TVar Database -> String -> String -> STM (Either (ErrString) [LogOperation])
alter_table_drop db tablename fieldname = do
  tvar_table <- get_table db tablename
  case tvar_table of 
    Just x -> do t <- readTVar x
                 case lookup fieldname (table t) of -- may have to unset primary key
                   Just f -> do writeTvar x (delete fieldname t)
                                let del_op = DeleteField fieldname tablename
                                if (primaryKey t)==fieldname
                                  then do modifyTvar x (set_primary_key t Nothing)
                                          return [del_op, SetPrimaryKey (Just fieldname) Nothing tablename]
                                  else return [del_op]
                   Nothing -> return ErrString (show(fieldname) ++ "does not exist in " ++ show(tablename)) 
    Nothing -> return ErrString (show(tablename) ++ " not found.") 
 
-- String is readable into list of tuples 
select :: TVar Database -> (forall a. [(Fieldname a, (a -> Bool))]) -> Tablename -> STM(Either (ErrString) String) -- last string is the stuff user queried for
{- select db fieldnames_and_conds tablename =  do
  tvar_table <- get_table db tablename
  case tvar_table of 
    Just x -> do t <- readTVar x
                 str_list = fmap (search_table t) field_names_and_conds
                 
                   return ErrString (show(fieldname) ++ "does not exist in " ++ show(tablename)) 
    Nothing -> return ErrString (show(tablename) ++ " not found.") -}

insert :: TVar Database -> Tablename -> (forall a. [(Fieldname a, a)]) -> STM(Either (ErrString) LogOperation) 
insert db tablename fieldnames_and_vals =

delete :: TVar Database -> Tablename -> (forall a. [(Fieldname a, (a -> Bool))]) -> STM(Either (ErrString) LogOperation)
delete db tablename filenames_and_conds =
 
update :: TVar Database -> Tablename -> (forall a. [(Fieldname a, a, (a -> Bool))]) -> STM (ErrString, LogOperation)
update db tablename filenames_vals_conds =
 
show_tables :: TVar Database -> STM (String) -- doesn't actually update the db, so no need for logstring
show_tables db = do
  hmdb <- readTVar db
  return foldl (\x -> (show (x) ++ )) "" (keys hmdb) 

