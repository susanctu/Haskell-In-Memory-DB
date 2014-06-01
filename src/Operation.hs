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

create_table :: TVar Database -> TransactionID -> Tablename -> (forall a. [(Fieldname a, Maybe a)]) -> Maybe (Fieldname b) -> STM (Either ErrString [LogOperation])
create_table db tr_id tablename field_and_col primary_key = do
  hmdb <- readTVar db
  if primary_key `elem` (map fst field_and_col)
    then case lookup tablename (database hmdb) of 
           Just x -> do let table_map = constructTableMap field_and_col empty 
                        insert tablename Table{primaryKey=primary_key, table=table_map} (database hmdb)
                        let op = CreateTable tr_id tablename
                        case primary_key of 
                          Nothing -> return [op] 
                          Just pk -> return [op, SetPrimaryKey tr_id Nothing primary_key tablename]
           Nothing -> return ErrString (show(tablename) ++ " already exists.")
    else return ErrString(show(primary_key) ++ "is not a valid primary key.") 

get_table :: TVar Database -> Tablename -> STM (Maybe (TVar Table))
get_table db tablename =  do 
  hmdb <- readTVar db
  return lookup tablename (database hmdb)    

drop_table :: TVar Database -> TransactionID -> Tablename -> STM (Either (ErrString) LogOperation)
drop_table db tr_id tablename = do 
  table <- get_table db tablename
  case table of 
    Just x -> do writeTvar db  (delete tablename (database hmdb))
                 return DropTable tr_id tablename  
    Nothing -> return ErrString (show(tablename) ++ " not found.")    


alter_table_add :: TVar Database -> TransactionID -> Tablename -> Fieldname a -> Maybe(Element a) -> Bool -> STM (Either (ErrString) [LogOperation]) 
alter_table_add db tr_id tablename fieldname default_val is_primary_key = do
  tvar_table <- get_table db tablename
  case tvar_table of 
    Just x -> do t <- readTVar x
                 case lookup fieldname (table t) of
                   Nothing -> do writeTvar x (insert fieldname Column{default_val=default_val,column=(Tvar empty)} t)
                                 let add_op = AddField tr_id fieldname tablename
                                 if is_primary_key
                                   then do let old_pk = primaryKey t
                                           modifyTvar x (set_primary_key t fieldname)
                                           return [add_op, SetPrimaryKey tr_id old_pk (Just fieldname) tablename]
                                   else return [add_op]
                   _ -> return ErrString (show(fieldname) ++ "already exists in " ++ show(tablename)) 
    Nothing -> return ErrString (show(tablename) ++ " not found.")    

{- Private -}
set_primary_key :: Table -> Fieldname a -> Table
set_primary_key (Table pk t) f = Table {primary_key=(Just f), table=t} 

{- Public -}
alter_table_drop :: TVar Database -> TransactionID -> Tablename -> Fieldname a -> STM (Either (ErrString) [LogOperation])
alter_table_drop db tr_id tablename fieldname = do
  tvar_table <- get_table db tablename
  case tvar_table of 
    Just x -> do t <- readTVar x -- t is of type Table
                 case lookup fieldname (table t) of -- may have to unset primary key
                   _ -> do writeTvar x (delete fieldname t)
                                let del_op = DeleteField tr_id fieldname tablename
                                if (primaryKey t)==fieldname
                                  then do modifyTvar x (set_primary_key t Nothing)
                                          return [del_op, SetPrimaryKey tr_id (Just fieldname) Nothing tablename]
                                  else return [del_op]
                   Nothing -> return ErrString (show(fieldname) ++ "does not exist in " ++ show(tablename)) 
    Nothing -> return ErrString (show(tablename) ++ " not found.") 

{- Private -}
check_elem :: (a -> Bool) -> Element a -> STM(Bool)
check_elem f e = (readTVar $ element e) >>= f

{- Private -}
filter_col :: Map RowHash (Element a) -> Maybe (a -> Bool) -> STM(Set.Set RowHash) 
filter_col c func = case func of 
  Nothing -> return c
  Just f -> do list_of_maybe_hashes <- mapM (\(h,x) -> if check_elem f x then Just h else Nothing) (toList c)
               return catMaybes list_of_maybe_hashes

{- Private -}
get_columns :: (forall a. Map (Fieldname a) (Column a)) -> (forall b. [Fieldname b]) -> (forall c. Map (Fieldname c) (c -> Bool)) -> (forall d. Map (Fieldname d) (Column d)) -> Set.Set RowHash -> STM(Either (Fieldname e) ((forall f. Map (Fieldname f) (Column f)), Set.Set RowHash))
get_columns table_map f:fieldnames fieldnames_and_conds map_so_far ignore_hashes = case lookup f table_map of 
  Just c -> do col_map <- readTVar column c
               ignore_hashes <- filter_col col_map $ lookup f fieldnames_and_conds
               let new_map = insert f new_col_map map_so_far
               return Right $ get_columns table_map fieldnames fieldnames_and_conds new_map ignore_hashes
  Nothing -> return Left f 
get_columns table_map _ map_so_far ignore_hashes = return Right (map_so_far, ignore_hashes) 

{- Private -}
get_row_for_hash :: (forall b. [(Fieldname b, Map RowHash (Element b))]) -> (forall a. Map (Fieldname a) (Column a)) -> Set.Set RowHash -> RowHash -> [STM String]
get_row_for_hash list_of_cols fieldname_to_col ignore_hashes hash = fmap (\m -> case lookup hash m of 
                                                                                  Nothing -> return $ show (lookup m fieldname_to_col)
                                                                                  Just x -> do val <- readTvar (element a)
                                                                                               return show(val)) (snd unzip list_of_cols)  

{- Public -}
-- Get the right columns, then filter columns individually, then create a set of rowhashes and output the rows that correspond to those rowhashes
-- resulting String is readable into list of lists
select :: TVar Database -> Tablename -> (forall a. [Fieldname a]) -> (Row -> Bool) -> STM(Either (ErrString) Table) -- last string is the stuff user queried for
select db tablename show_fieldnames fieldnames_and_conds =  do
  tvar_table <- get_table db tablename
  case tvar_table of 
    Just x -> do t <- readTVar x -- t is of type Table
                 cols <- get_columns (table t) show_fieldnames fieldnames_and_conds empty
                 case cols of 
                   Left err_str -> return Left ErrString(show(err_str) ++ " not present in " ++ show(tablename))
                   Right (col_map, ignore_hashes, all_hashes) -> let list_of_rows = map (get_row_for_hash zip(show_fieldnames col_map) (table t) ignore_hashes) all_hashes
                                                                   in do list_of_lists <- sequence (map sequence list_of_rows) 
                                                                         return show(list_of_lists)
    Nothing -> return ErrString (show(tablename) ++ " not found.")

join :: TVar Database -> Tablename -> Tablename -> (Row -> Row -> Bool) -> Table 

insert_vals :: TransactionID -> RowHash -> Tablename -> Table -> (forall a. [(Fieldname a, a)]) -> (forall b. Set.Set (Fieldname b)) -> [LogOperation] -> Either (ErrString) (Table, [LogOperation])
insert_vals tr_id rowhash tablename t (fieldname, val):xs seen_fieldnames logOps = if fieldname `member` seen_fieldnames
  then (show(fieldname) ++ " used twice in same insert into " ++ show(tablename)) 
  else case lookup fieldname (table t) of
         Nothing -> (show(fieldname) ++ " does not exist in " ++ show(tablenames))
         Just c -> insert_vals tr_id tablename (insert rowhash Element{elem=newTvar val} (column c)) xs (TransactionLog tr_id (tablename, fieldname, rowhash) Nothing (Just val)):logOps
insert_vals t _ = t

get_fields_with_defaults :: TVar Database -> 

-- remove RowHash, keep per-table counter
insert :: TVar Database -> TransactionID -> Tablename -> (forall a. [(Fieldname a, a)]) -> STM(Either (ErrString) [LogOperation]) 
insert db tr_id tablename fieldnames_and_vals = do
  tvar_table <- get_table db tablename
  case tvar_table of 
    Just x -> do t <- readTVar x -- t is of type Table
                 fields_without_defaults <- get_fields_without_defaults
                 foldr (\arg1 arg2 -> `member` fieldnames) fields_without_defaults
                 case insert_vals tr_id rowhash tablename t fieldnames_and_vals [] of 
                   Left err_str -> return err_str 
                   Right (new_t, logOps) -> do writeTvar x new_t
                                               return logOps
    Nothing -> return ErrString (show(tablename) ++ " not found.")    
 

{- I do not plan on implementing these next two until I get everything else to compile -}
delete :: TVar Database -> TransactionID -> Tablename -> (Row -> Bool) -> STM(Either (ErrString) LogOperation)
delete db tr_id tablename fieldnames_and_conds =

 
update :: TVar Database -> TransactionID -> Tablename -> (Row -> Bool) -> (Row -> Row) -> STM (Either ErrString LogOperation)
update db tr_id tablename fieldnames_vals_conds = 

show_tables :: TVar Database -> STM (String) -- doesn't actually update the db, so no need for logstring
show_tables db = do
  hmdb <- readTVar db
  return foldl (\x -> (show (x) ++ )) "" (keys hmdb) 

