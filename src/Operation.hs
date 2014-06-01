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
select :: TVar Database -> Tablename -> [Fieldname] -> (Row -> Bool) -> STM(Either (ErrString) Table) -- last string is the stuff user queried for
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

{-Public-}
{- join :: TVar Database -> Tablename -> Tablename -> (Row -> Row -> Bool) -> Table -}

{-Private: insert values int to the table, keeping track of what columns you have already
  inserted into in order to check that we are not inserting twice into the same column. Also
  collects and returns the appropriate LogOperations.
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
                    let new_col = Column (default_val c) (col_type c) (insert rowhash (Just new_elem) (col_map)) 
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
                        then case insert_vals tr_id ((rowCounter t) + 1) tablename t row empty [] of 
                               Left err_str -> return err_str 
                               Right (new_t, logOps) -> do writeTvar x new_t
                                                           return logOps
                         else return ErrString ("Failed to provide values for all columns in " + show(tablename) + " with no default")
    Nothing -> return ErrString (show(tablename) ++ " not found.")    
 
{- get_row_for_hash :: Table -> RowHash -> Row
get_row_for_hash :: t rhash = Row func
     where func x = lookup col 'x' in Table and then look for rhash -}


{- I do not plan on implementing these next two until I get everything else to compile, but these are the intended function prototypes -}
{- delete :: TVar Database -> TransactionID -> Tablename -> (Row -> Bool) -> STM(Either (ErrString) LogOperation)
delete db tr_id tablename fieldnames_and_conds =

 
update :: TVar Database -> TransactionID -> Tablename -> (Row -> Bool) -> (Row -> Row) -> STM (Either ErrString LogOperation)
update db tr_id tablename fieldnames_vals_conds = -}

{-Public-}
show_tables :: TVar Database -> STM (String) -- doesn't actually update the db, so no need for logstring
show_tables db = do
  hmdb <- readTVar db
  return foldl (\x -> (show (x) ++ )) "" (keys table hmdb) 

