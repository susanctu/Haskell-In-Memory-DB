{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ExistentialQuantification #-} 
{-# LANGUAGE RankNTypes #-}
module Operation (create_table, drop_table, alter_table_add, alter_table_drop, select, Operation.insert, Operation.update, Operation.delete, show_tables) where

import System.IO
import Control.Monad
import Control.Concurrent
import Control.Concurrent.STM
import Data.Map.Lazy

{-Private: Constructs the map inside a table-}
constructTableMap :: [(Fieldname, Maybe Element, TypeRep)] -> Map Fieldname Column -> Map Fieldname Column
constructTableMap (fname, default_val, col_type):xs old_map = let col = Tvar empty 
  in constructTableMap xs (insert fname (Column {default_val=default_val, col_type=col_type, column=col})) 
constructTableMap _ old_map = old_map

{-Public-}
create_table :: TVar Database -> TransactionID -> Tablename -> [(Fieldname, Maybe Element, TypeRep)] -> Maybe Fieldname -> STM (Either ErrString [LogOperation])
create_table db tr_id tablename field_and_default (Maybe pk) = if pk `elem` fst unzip field_and_col
  then create_table_validated_pk db tr_id tablename field_and_default (Maybe pk)
  else return ErrString(show(pk) ++ "is not a valid primary key.") 
create_table db tr_id tablename field_and_default Nothing = create_table_validated_pk db tr_id tablename field_and_default Nothing

{-Private: The rest of create_table's functionality after primary key has already been verified-}
create_table_with_validated_pk :: TVar Database -> TransactionID -> Tablename -> [(Fieldname, Maybe Element, TypeRep)] -> Maybe Fieldname -> STM (Either ErrString [LogOperation])
create_table_validated_pk create_table db tr_id tablename field_and_default primary_key = do
  hmdb <- readTVar db
  case lookup tablename (database hmdb) of 
           Just _ -> do let table_map = constructTableMap field_and_default empty 
                        tvar_table <- newTvar Table{rowCounter=0, primaryKey=primary_key, table=table_map}
                        insert tablename tvar_table (database hmdb)
                        let op = CreateTable tr_id tablename
                        return [op, SetPrimaryKey tr_id Nothing primary_key tablename]
           Nothing -> return ErrString (show(tablename) ++ " already exists.")
    
{-Private: Get the specified TVar Table from the database, may return Nothing-}
get_table :: TVar Database -> Tablename -> STM (Maybe (TVar Table))
get_table db tablename =  do 
  hmdb <- readTVar db
  return lookup tablename (database hmdb)    

{-Public: Drop the specified table-}
drop_table :: TVar Database -> TransactionID -> Tablename -> STM (Either (ErrString) LogOperation)
drop_table db tr_id tablename = do 
  table <- get_table db tablename
  case table of 
    Just _ -> do writeTvar db  (delete tablename (database hmdb))
                 return DropTable tr_id tablename  
    Nothing -> return ErrString (show(tablename) ++ " not found.")    

{-Public: Add a field to a table, with optional specification of default value and primary key-}
alter_table_add :: TVar Database -> TransactionID -> Tablename -> Fieldname -> TypeRep -> Maybe Element -> Bool -> STM (Either (ErrString) [LogOperation]) 
alter_table_add db tr_id tablename fieldname col_type default_val is_primary_key = do
  tvar_table <- get_table db tablename
  case tvar_table of 
    Just x -> do t <- readTVar x
                 case lookup fieldname (table t) of
                   Nothing -> let add_op = AddField tr_id fieldname tablename
                                in if is_primary_key
                                     then do writeTvar x $ Table (rowCounter t) (Just fieldname) (insert fieldname Column{default_val=default_val,col_type=col_type, column=(Tvar empty)} t)
                                             return [add_op, SetPrimaryKey tr_id old_pk (Just fieldname) tablename]
                                     else do writeTvar x $ Table (rowCounter t) (primaryKey t) (insert fieldname Column{default_val=default_val,col_type=col_type, column=(Tvar empty)} t)
                                             return [add_op]
                   _ -> return ErrString (show(fieldname) ++ "already exists in " ++ show(tablename)) 
    Nothing -> return ErrString (show(tablename) ++ " not found.")    

{- Public -}
alter_table_drop :: TVar Database -> TransactionID -> Tablename -> Fieldname -> STM (Either (ErrString) [LogOperation])
alter_table_drop db tr_id tablename fieldname = do
  tvar_table <- get_table db tablename
  case tvar_table of 
    Just x -> do t <- readTVar x -- t is of type Table
                 case lookup fieldname (table t) of -- may have to unset primary key
                   Just _ -> do let del_op = DeleteField tr_id fieldname tablename
                                  in if (primaryKey t)==fieldname 
                                       then do writeTvar x $ Table (rowCounter t) Nothing (delete fieldname (table t))
                                               return [del_op, SetPrimaryKey tr_id (Just fieldname) Nothing tablename]
                                       else do writeTvar x $ Table (rowCounter t) (primaryKey t) (delete fieldname (table t))
                                               return [del_op]
                   Nothing -> return ErrString (show(fieldname) ++ "does not exist in " ++ show(tablename)) 
    Nothing -> return ErrString (show(tablename) ++ " not found.") 

{- Private -}
-- Each row is completely stored (i.e. you would get something if you looked up the rowhash in every column)
-- so we take all the rowhashes, construct Rows for every row, only keep the ones that we get true for
-- when we pass them to the (Row -> Bool) function. 
-- returns the hashes of all the rows we want to show, and the rows themselves 
get_rows :: Map Fieldname Column -> (Row -> Bool)-> STM([RowHash], [Row])
get_rows table_map conds all_hashes = do
  all_hashes <- let tvar_c = head elems table_map in do c <- readTVar tvar_c 
                                                        return $ keys column c 
  lst_of_tuples <- mapM (construct_row_for_hash) all_hashes 
  return unzip $ filter (\(h, r) -> cond r ) lst_of_tuples


{-Private-}
construct_row_for_hash :: Map Fieldname Column -> RowHash -> STM(RowHash, Row)
construct_row_for_hash table_map rowhash = (rowhash, Row func)
  where func fieldname = case lookup fieldname table_map of 
                           Just tvar_c -> do c <- readTVar tvar_c
                                             case lookup rowhash c of 
                                               Nothing -> return Nothing
                                               Just tvar_elem -> do e <- readTVar tvar_elem 
                                                                    return Just e 
                           Nothing -> return Nothing 

{-Public:
  A thin wrapper around construct_row_for_hash-}
get_row_for_hash :: Table -> RowHash -> Row
get_row_for_hash :: t rhash = construct_row_for_hash (table t) rhash 

{-Public, incomplete-}
get_column_type :: TVar Database -> Tablename -> Fieldname -> TypeRep
get_column_type db tablename fieldname = 

{-Public-}
get_column_default :: TVar Database -> Tablename -> Fieldname -> Maybe Element

{-Private: Recursively insert all the provided rows-}
insert_all_vals :: TransactionID -> [RowHash] -> Tablename -> Table -> [Fieldnames] -> [Row] -> Either ErrString Table
insert_all_vals tr_id rh:rowhash tablename t fieldnames r:rows = case insert_vals tr_id r tablename t fieldnames r [] of 
  Left str -> Left str 
  Right (new_t,_) -> Right new_t
insert_all_vals _ _ _ t _ _ = t

{- Public -}
-- Checks that fieldnames exist in the specified table
-- Grab the columns then for all the rowhashes that we do not ignore 
-- Make a Table (using insert_vals) with all those elements and return it
select :: TVar Database -> Tablename -> [Fieldname] -> (Row -> Bool) -> STM(Either ErrString Table)
select db tablename show_fieldnames cond =  do
  tvar_table <- get_table db tablename
  case tvar_table of 
    Just x -> do t <- readTVar x -- t is of type Table
                 (show_hashes, rows) <- get_rows (table t) cond
                 let table_map = constructTableMap (fmap (\fname -> (fname, get_column_default fname, get_column_type fname)) show_fieldnames)
                     new_table = Table{rowCounter=(rowCounter t), primaryKey=(primary_key t), table=table_map}
                     dummy_id = TransactionID ""
                     in case insert_all_vals dummy_id show_hashes tablename new_table show_fieldnames rows
                          Left str -> return Left str
                          Right filled_table -> return Right filled_table
    Nothing -> return ErrString (show(tablename) ++ " not found.")

{-Public-}
join :: TVar Database -> Tablename -> Tablename -> (Row -> Row -> Bool) -> Table

{-Private: insert values int to the table for the columns specified by [Fieldnames].
  Note that if you're operating on a table in the database, that means you should pass in all the fieldnames
  present in the table. If you're trying to construct the table returned by select or join, you may be passing in only some of the fieldnames.
  We only check for the fieldnames passed in that you actually specified a Non-Nothing value in the absence of a Just-wrapped default value
  for the column.
  Also collects and returns the appropriate LogOperations.
-}
insert_vals :: TransactionID -> RowHash -> Tablename -> Table -> [Fieldnames] -> Row -> [LogOperation] -> Either (ErrString) (Table, [LogOperation])
insert_vals tr_id rowhash tablename t f:fieldnames row logOps = 
  case (getField row) f of 
    Just new_elem -> do res <- insert_val_helper tr_id rowhash tablename t f new_elem
                        case res of 
                          Left str -> return str
                          Right (new_table, new_logOp) -> insert_vals tr_id rowhash tablename t fieldnames row new_logOp:logOps
    Nothing -> case default_val c of
                 Nothing -> return ErrString ("DB error")
                 Just val -> do res <- insert_val_helper tr_id rowhash tablename t f val
                                case res of 
                                  Left str -> return str
                                  Right (new_table, new_logOp) -> insert_vals tr_id rowhash tablename t fieldnames row new_logOp:logOps   
insert_vals _ _ _ t _ _ logOps = (t, logOps)

{-Private-}
insert_val_helper :: TransactionID -> RowHash -> Tablename -> Table -> Fieldname -> Element -> STM(Either ErrString (Table, LogOperation))
insert_val_helper tr_id rowhash tablename t f new_elem = let new_logOp = TransactionLog tr_id (tablename, f, rowhash) Nothing (Just new_elem) 
  in case lookup f (table t) of
       Just c -> do col_map <- readTVar $ column c
                    new_tvar_elem <- newTvar Just new_elem
                    let new_col = Column (default_val c) (col_type c) (insert rowhash new_tvar_elem (col_map)) 
                    let new_table = Table (rowCounter t) (primaryKey t) (insert fieldname new_col (table t))
                    return (new_table, new_logOp)
       Nothing -> return ErrString "DB error"

{-Private: Get a list of fieldnames for which default_val was Nothing-}
get_fields_without_defaults :: Table -> [Fieldname]
get_fields_without_defaults t = let maybe_fieldnames = mapWithKey (\k v -> if (default_val v == Nothing) then Nothing else Just k) (table t) in
  catMaybes maybe_fieldnames

{-Private-}
get_all_fields :: Table -> [Fieldnames]
get_all_fields t = keys table t

{-Private-}
check_unique :: [Fieldnames] -> Bool
check_unique fieldnames = length fieldnames == size Set.fromList fieldnames

{-Public: Inserts a row into a table
  Handles the following error case:
  * user fails to specify value for a column with a default value of Nothing 
  We do not handle (these should have been taken care of in the construction of Row)
  * user names the same column twice 
  * user names an invalid column
-}
insert :: TVar Database -> TransactionID -> Tablename -> Row -> STM(Either (ErrString) [LogOperation]) 
insert db tr_id tablename row = do
  tvar_table <- get_table db tablename
  case tvar_table of 
    Just x -> do t <- readTVar x -- t is of type Table
                 fields_without_defaults <- get_fields_without_defaults t 
                 let has_required_vals = foldr (\fname bool -> bool && ((getField row) fname != Nothing)) True $ get_fields_without_defaults t
                   in if has_required_vals
                        then case insert_vals tr_id ((rowCounter t) + 1) tablename t (get_all_fields t) row [] of 
                               Left err_str -> return err_str 
                               Right (new_t, logOps) -> do writeTvar x new_t
                                                           return logOps
                         else return ErrString ("Failed to provide values for all columns in " + show(tablename) + " with no default")
    Nothing -> return ErrString (show(tablename) ++ " not found.")    

{- I do not plan on implementing these next two until I get everything else to compile, but these are the intended function prototypes -}
delete :: TVar Database -> TransactionID -> Tablename -> (Row -> Bool) -> STM(Either (ErrString) LogOperation)
delete db tr_id tablename conds =

 
update :: TVar Database -> TransactionID -> Tablename -> (Row -> Bool) -> (Row -> Row) -> STM (Either ErrString LogOperation)
update db tr_id tablename conds changes =

{-Public-}
show_tables :: TVar Database -> STM (String) -- doesn't actually update the db, so no need for logstring
show_tables db = do
  hmdb <- readTVar db
  return foldl (\x -> (show (x) ++ )) "" (keys table hmdb) 

