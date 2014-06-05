{-# LANGUAGE GeneralizedNewtypeDeriving #-}
module Operation (create_table
  , drop_table
  , alter_table_add
  , alter_table_drop
  , select
  , insert
  , show_tables
  , update
  , delete) where

import Control.Monad
import Control.Concurrent.STM
import qualified Data.Map.Lazy as L
import Data.Typeable
import Data.Maybe
import DBTypes
import DBUtils
import qualified Data.Foldable as M

{-Private: Constructs the map inside a table-}
constructTableMap :: [(Fieldname, Maybe Element, TypeRep)] -> L.Map Fieldname Column -> STM(L.Map Fieldname Column)
constructTableMap ((fname, default_val_arg, col_type_arg):xs) old_map = do 
  col <- newTVar L.empty 
  constructTableMap xs (L.insert fname (Column {default_val=default_val_arg, col_type=col_type_arg, column=col}) old_map) 
constructTableMap _ old_map = return old_map

{-Public-}
create_table :: TVar Database -> TransactionID -> Tablename -> [(Fieldname, Maybe Element, TypeRep)] -> Maybe Fieldname -> STM (Either ErrString [LogOperation])
create_table db tr_id tablename field_and_default (Just pk) = if pk `elem` (fmap (\(x,_,_) -> x) field_and_default)
  then create_table_with_validated_pk db tr_id tablename field_and_default (Just pk)
  else return $ Left $ ErrString(show(pk) ++ "is not a valid primary key.") 
create_table db tr_id tablename field_and_default Nothing = create_table_with_validated_pk db tr_id tablename field_and_default Nothing

{-Private: The rest of create_table's functionality after primary key has already been verified-}
create_table_with_validated_pk :: TVar Database -> TransactionID -> Tablename -> [(Fieldname, Maybe Element, TypeRep)] -> Maybe Fieldname -> STM (Either ErrString [LogOperation])
create_table_with_validated_pk db tr_id tablename field_and_default primary_key = do
  hmdb <- readTVar db
  case L.lookup tablename (database hmdb) of 
           Just _ -> do table_map <- constructTableMap field_and_default L.empty 
                        tvar_table <- newTVar Table{rowCounter=0, primaryKey=primary_key, table=table_map}
                        let new_db = L.insert tablename tvar_table (database hmdb)
                        writeTVar db (Database new_db)
                        let op = CreateTable tr_id tablename
                        return $ Right [op, SetPrimaryKey tr_id Nothing primary_key tablename]
           Nothing -> return $ Left $ ErrString (show(tablename) ++ " already exists.")
    
{-Private: Get the specified TVar Table from the database, may return Nothing-}
get_table :: TVar Database -> Tablename -> STM (Maybe (TVar Table))
get_table db tablename =  do 
  hmdb <- readTVar db
  return $ L.lookup tablename (database hmdb)    

{-Public: Drop the specified table-}
drop_table :: TVar Database -> TransactionID -> Tablename -> STM (Either (ErrString) [LogOperation])
drop_table db tr_id tablename_arg = do 
  hmdb <- readTVar db
  table <- get_table db tablename_arg
  case table of 
    Just _ -> do writeTVar db  $ Database (L.delete tablename_arg (database hmdb))
                 return $ Right $ [DropTable tr_id tablename_arg]
    Nothing -> return $ Left $ ErrString (show(tablename_arg) ++ " not found.")    

{-Public: Add a field to a table, with optional specification of default value and primary key-}
alter_table_add :: TVar Database -> TransactionID -> Tablename -> Fieldname -> TypeRep -> Maybe Element -> Bool -> STM (Either (ErrString) [LogOperation]) 
alter_table_add db tr_id tablename fieldname col_type default_val is_primary_key = do
  tvar_table <- get_table db tablename
  case tvar_table of 
    Just x -> do t <- readTVar x
                 case L.lookup fieldname (table t) of
                   Nothing -> let add_op = AddField tr_id tablename fieldname
                                in if is_primary_key
                                     then do let old_pk = primaryKey t
                                             new_col <- newTVar L.empty
                                             writeTVar x $ Table (rowCounter t) (Just fieldname) (L.insert fieldname Column{default_val=default_val,col_type=col_type, column=new_col} (table t))
                                             return $ Right [add_op, SetPrimaryKey tr_id old_pk (Just fieldname) tablename]
                                     else do new_col <- newTVar L.empty
                                             writeTVar x $ Table (rowCounter t) (primaryKey t) (L.insert fieldname Column{default_val=default_val,col_type=col_type, column=new_col} (table t))
                                             return $ Right [add_op]
                   _ -> return $ Left $ ErrString (show(fieldname) ++ "already exists in " ++ show(tablename)) 
    Nothing -> return $ Left $ ErrString (show(tablename) ++ " not found.")    

{- Public -}
alter_table_drop :: TVar Database -> TransactionID -> Tablename -> Fieldname -> STM (Either (ErrString) [LogOperation])
alter_table_drop db tr_id tablename fieldname = do
  tvar_table <- get_table db tablename
  case tvar_table of 
    Just x -> do t <- readTVar x -- t is of type Table
                 case L.lookup fieldname (table t) of -- may have to unset primary key
                   Just _ -> do let del_op = DropField tr_id tablename fieldname
                                  in if (primaryKey t)==(Just fieldname) 
                                       then do writeTVar x $ Table (rowCounter t) Nothing (L.delete fieldname (table t))
                                               return $ Right [del_op, SetPrimaryKey tr_id (Just fieldname) Nothing tablename]
                                       else do writeTVar x $ Table (rowCounter t) (primaryKey t) (L.delete fieldname (table t))
                                               return $ Right [del_op]
                   Nothing -> return $ Left $ ErrString (show(fieldname) ++ "does not exist in " ++ show(tablename)) 
    Nothing -> return $ Left(ErrString (show(tablename) ++ " not found."))

{- Private -}
-- Each row is completely stored (i.e. you would get something if you looked up the rowhash in every column)
-- so we take all the rowhashes, construct Rows for every row, only keep the ones that we get true for
-- when we pass them to the (Row -> Bool) function. 
-- returns the hashes of all the rows we want to show, and the rows themselves 
get_rows :: L.Map Fieldname Column -> (Row -> STM(Bool))-> STM([RowHash], [Row])
get_rows table_map cond = do
  all_hashes <- let c = head (L.elems table_map) in do col_map <- readTVar $ column c 
                                                       return $ L.keys col_map
  lst_of_tuples <- mapM (construct_row_for_hash table_map) all_hashes 
  filtered <- filterM (\(_, r) -> cond r ) lst_of_tuples
  return $ unzip $ filtered

{-Private-}
construct_row_for_hash :: L.Map Fieldname Column -> RowHash -> STM(RowHash, Row)
construct_row_for_hash table_map rowhash = return (rowhash, Row func)
  where func fieldname = case L.lookup fieldname table_map of 
                           Just col -> do c <- readTVar $ column col
                                          case L.lookup rowhash c of 
                                            Nothing -> return Nothing
                                            Just tvar_elem -> do e <- readTVar tvar_elem 
                                                                 return $ Just e 
                           Nothing -> return Nothing 

{-Public:
  A thin wrapper around construct_row_for_hash-}
get_row_for_hash :: Table -> RowHash -> STM(Row)
get_row_for_hash t rhash = do (_, r) <- construct_row_for_hash (table t) rhash 
                              return r

{-Private-}
get_column:: TVar Database -> Tablename -> Fieldname -> STM(Maybe Column)
get_column db tablename fieldname = do
  maybe_tvar_table <- get_table db tablename
  case maybe_tvar_table of 
    Just tvar_table -> do t <- readTVar tvar_table
                          return $ L.lookup fieldname (table t) 
    Nothing -> return Nothing

{-Public-}
get_column_type :: TVar Database -> Tablename -> Fieldname -> STM(Maybe TypeRep)
get_column_type db tablename fieldname = do
  maybe_col <- get_column db tablename fieldname 
  case maybe_col of 
    Just col -> return $ Just $ col_type col  
    Nothing -> return Nothing

{-Public-}
get_column_default :: TVar Database -> Tablename -> Fieldname -> STM(Maybe Element)
get_column_default db tablename fieldname = do
  maybe_col <- get_column db tablename fieldname
  case maybe_col of 
    Just col -> return $ default_val col  
    Nothing -> return Nothing

{-Private: Recursively insert all the provided rows-}
insert_all_vals :: TransactionID -> [RowHash] -> Tablename -> Table -> [Fieldname] -> [Row] -> STM(Either ErrString Table)
insert_all_vals tr_id (rh:rowhash) tablename t fieldnames (r:rows) = do
  res <- insert_vals tr_id rh tablename t fieldnames r [] 
  case res of 
    Left str -> return $ Left str 
    Right (new_t, _) -> insert_all_vals tr_id rowhash tablename new_t fieldnames rows
insert_all_vals _ _ _ t _ _ = return $ Right t 

{-Private-}
construct_col_info ::  Table -> Fieldname -> STM (Maybe (Fieldname,Maybe Element,TypeRep))
construct_col_info t fieldname = case L.lookup fieldname (table t) of
  Just col -> return $ Just (fieldname, default_val col, col_type col)
  Nothing -> return Nothing 

{- Public -}
-- Checks that fieldnames exist in the specified table
-- Grab the columns then for all the rowhashes that we do not ignore 
-- Make a Table (using insert_vals) with all those elements and return it
select :: TVar Database -> Tablename -> [Fieldname] -> (Row -> STM(Bool)) -> STM(Either ErrString Table)
select db tablename show_fieldnames cond =  do
  tvar_table <- get_table db tablename
  case tvar_table of 
    Just x -> do t <- readTVar x -- t is of type Table
                 (show_hashes, rows) <- get_rows (table t) cond
                 col_info <- sequence $ fmap (construct_col_info t) show_fieldnames -- list of 3-uples
                 table_map <-  constructTableMap (catMaybes col_info) L.empty
                 let new_table = Table{rowCounter=(rowCounter t), primaryKey=(primaryKey t), table=table_map}
                     dummy_id = TransactionID "" 0
                     in do res <- insert_all_vals dummy_id show_hashes tablename new_table show_fieldnames rows 
                           case res of
                             Left str -> return $ Left str
                             Right filled_table -> return $ Right filled_table
    Nothing -> return $ Left $ ErrString (show(tablename) ++ " not found.")

{-Public-}
{-join :: TVar Database -> Tablename -> Tablename -> (Row -> Row -> Bool) -> Table-}

{-Private: insert values int to the table for the columns specified by [Fieldnames].
  Note that if you're operating on a table in the database, that means you should pass in all the fieldnames
  present in the table. If you're trying to construct the table returned by select or join, you may be passing in only some of the fieldnames.
  We only check for the fieldnames passed in that you actually specified a Non-Nothing value in the absence of a Just-wrapped default value
  for the column.
  Also collects and returns the appropriate LogOperations.
-}
insert_vals :: TransactionID -> RowHash -> Tablename -> Table -> [Fieldname] -> Row -> [(Fieldname, Element)] -> STM(Either ErrString (Table, [(Fieldname,Element)]))
insert_vals tr_id rowhash tablename t (f:fieldnames) row logOps = do
  maybe_elem <- (getField row) f
  case maybe_elem of 
    Just new_elem -> do res <- insert_val_helper tr_id rowhash tablename t f new_elem
                        case res of 
                          Left str -> return $ Left str
                          Right (new_table, new_logOp) -> insert_vals tr_id rowhash tablename t fieldnames row (new_logOp:logOps)
    Nothing -> case L.lookup f (table t) of
                 Nothing -> return $ Left $ ErrString ("DB error")
                 Just col -> case default_val col of 
                               Just val -> do res <- insert_val_helper tr_id rowhash tablename t f val
                                              case res of 
                                                Left str -> return $ Left str
                                                Right (new_table, new_logOp) -> insert_vals tr_id rowhash tablename t fieldnames row (new_logOp:logOps)   
                               Nothing -> return $ Left $ ErrString "DB error"
insert_vals _ _ _ t _ _ logOps = return $ Right (t, logOps)

{-Private-}
insert_val_helper :: TransactionID -> RowHash -> Tablename -> Table -> Fieldname -> Element -> STM (Either ErrString (Table, (Fieldname,Element))) 
insert_val_helper tr_id rowhash tablename t f new_elem = let new_logOp = (f, new_elem) 
  in case L.lookup f (table t) of
       Just c -> do col_map <- readTVar $ column c
                    new_tvar_elem <- newTVar new_elem
                    new_col_map <- newTVar $ L.insert rowhash new_tvar_elem (col_map)
                    let new_col = Column (default_val c) (col_type c) new_col_map
                    let new_table = Table ((rowCounter t) +1) (primaryKey t) (L.insert f new_col (table t))
                    return $ Right (new_table, new_logOp)
       Nothing -> return $ Left (ErrString "DB error")

{-Private: Get a list of fieldnames for which default_val was Nothing-}
get_fields_without_defaults :: Table -> [Fieldname]
get_fields_without_defaults t = let maybe_fieldnames = L.mapWithKey (\k v -> if (isNothing(default_val v)) then Nothing else Just k) (table t) in
  catMaybes (L.elems maybe_fieldnames)

{-Private-}
get_all_fields :: Table -> [Fieldname]
get_all_fields t = L.keys $ table t

{-Private-}
check_defaults :: Row -> Fieldname -> Bool -> STM(Bool) 
check_defaults row fname bool = liftM (bool &&) $ liftM isNothing ((getField row) fname)

{-Public: Inserts a row into a table
  Handles the following error case:
  * user fails to specify value for a column with a default value of Nothing 
  We do not handle (these should have been taken care of in the construction of Row)
  * user names the same column twice 
  * user names an invalid column
-}
insert :: TVar Database -> TransactionID -> Tablename -> Row -> STM(Either ErrString LogOperation) 
insert db tr_id tablename row = do
  tvar_table <- get_table db tablename
  case tvar_table of 
    Just x -> do t <- readTVar x -- t is of type Table
                 has_required_vals <- M.foldrM (check_defaults row) True $ get_fields_without_defaults t
                 if has_required_vals
                    then do res <- insert_vals tr_id (RowHash((rowCounter t) + 1)) tablename t (get_all_fields t) row []
                            case res of 
                              Left err_str -> return $ Left err_str 
                              Right (new_t, logOps) -> do writeTVar x new_t
                                                          return $ Right $ Insert tr_id (tablename, RowHash((rowCounter t) + 1)) logOps
                    else return $ Left(ErrString ("Failed to provide values for all columns in " ++ show(tablename) ++ " with no default"))
    Nothing -> return $ Left(ErrString (show(tablename) ++ " not found."))    

{-Public-}
show_table_contents :: Table -> STM(String)
show_table_contents t = let pk = show(primaryKey t) 
                            fieldnames = L.keys $ table t
                            scheme = fmap show $ fieldnames 
                            in do (_, rows) <- get_rows (table t) (verify_row (fmap (\f -> (f, func)) fieldnames)) 
                                  row_strs <- sequence $ fmap (printRow fieldnames) rows 
                                  return $ "Primary key:" ++ pk ++ "\n" ++ unwords(scheme) ++ "\n" ++ unlines(row_strs)
                               where func element = True

{-Private-}
printRow :: [Fieldname] -> Row -> STM(String)
printRow fieldnames row = do maybe_elems <- sequence $ fmap (getField row) fieldnames  
                             return $ unwords $ fmap show maybe_elems 

{-Private-}
--delete all the hashes from the given column and return the resulting column
delete_hashes_from_column :: [RowHash] -> (Fieldname, Column) -> STM(Fieldname, Column) 
delete_hashes_from_column hashes (fieldname, col) = do
  col_map <- readTVar $ column col 
  tvar_col <- newTVar (delete_hashes_from_col_map hashes col_map)
  return (fieldname, Column{default_val=(default_val col), col_type=(col_type col),column=tvar_col})

{-Private-}
delete_hashes_from_col_map :: [RowHash] -> L.Map RowHash (TVar Element) -> L.Map RowHash (TVar Element) -- could return deleted vals for each hash in that col
delete_hashes_from_col_map (h:hashes) col_map = delete_hashes_from_col_map hashes $ L.delete h col_map 
delete_hashes_from_col_map _ col_map = col_map

{-Public-}
delete :: TVar Database -> TransactionID -> Tablename -> (Row -> STM Bool) -> STM(Either (ErrString) [LogOperation])
delete db tr_id tablename conds = do
  tvar_table <- get_table db tablename
  case tvar_table of 
    Just x -> do t <- readTVar x -- t is of type Table
                 (delete_hashes, rows) <- get_rows (table t) conds
                 let fieldnames = (get_all_fields t)
                 list_of_list_of_elems <- (sequence $ fmap (\r -> liftM catMaybes (mapM (getField r) fieldnames)) rows)::STM([[Element]])
                 list_for_table_map <- mapM (delete_hashes_from_column delete_hashes) $ L.toList(table t)
                 let new_table_map = L.fromList list_for_table_map
                 writeTVar x Table{rowCounter=(rowCounter t), primaryKey=(primaryKey t), table=new_table_map}
                 return $ Right $ fmap (\(hash, list_of_elems) -> Delete tr_id (tablename, hash) (zip fieldnames list_of_elems)) $ zip delete_hashes list_of_list_of_elems      
    Nothing -> return $ Left $ ErrString (show(tablename) ++ " not found.")
 
update :: TVar Database -> TransactionID -> Tablename -> (Row -> STM Bool) -> (Row -> Row) -> STM (Either ErrString LogOperation)
update db tr_id tablename conds changes = return $ Left $ ErrString "unimplemented!"

{-Public-}
show_tables :: TVar Database -> STM (String) -- doesn't actually update the db, so no need for logstring
show_tables db = do
  hmdb <- readTVar db
  return $ foldl (\x y -> (x ++ show(y))) "" (L.keys (database hmdb)) 

