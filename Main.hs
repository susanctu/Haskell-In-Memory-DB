module Main where

import qualified Operation as O
import qualified DBTypes as D 
import qualified DBUtils as DU
import Control.Concurrent.STM
import qualified Data.Map.Lazy as L 
import Data.Typeable
import qualified DiskManager as DM
import Data.Set as S

create_empty_db :: STM(TVar D.Database)
create_empty_db = newTVar $ D.Database L.empty

main :: IO ()
main = do output <- sequence $ [test_create_table
                                ,test_drop_table
                                ,test_alter_add
                                ,test_alter_drop
                                ,test_insert
                                ,test_select
                                ,test_delete
                                ,test_update
                                ,test_write_and_hydrate_more
                                --,test_write_and_hydrate
                                ]
          _ <- mapM putStrLn output
          return ()

{-
  These were some really simple tests that we used to inspect whether the operations in the Operation module
  and whether the deserialization and serialization functionality provided the DiskManager module worked properly in at least typical cases.
  Unfortunately we did not finish our parser so we were never able to test these from the client side. 
 -}

--This test creates a table with two fields with default values and no primary key.
--This should return the schema information in string form.
test_create_table :: IO String
test_create_table = atomically $ do let tablename  = D.Tablename "sample_table"
                                    let field_and_default =  [(D.Fieldname "field1", 1), (D.Fieldname "field2", 2)]
                                    db <- create_empty_db
                                    res <- O.create_table db (D.TransactionID "blah" 0) tablename (fmap (\(f, d)-> (f, Just(D.Element(Just d)), typeOf(d::Int))) field_and_default) Nothing
                                    case res of 
                                      Left (D.ErrString errstr) -> return errstr
                                      Right _ -> O.show_table_contents db tablename

--This test creates two tables, one with two cols and one with just one, and drops one of them. 
--This should return a string showing all the schemas before one table was dropped, and all the schemas afterward.
test_drop_table :: IO String
test_drop_table = atomically $ do let tablename1  = D.Tablename "sample_table1"
                                  let tablename2 = D.Tablename "sample_table2"
                                  let field_and_default1 =  [(D.Fieldname "field1", 1), (D.Fieldname "field2", 2)]
                                  let field_and_default2 = [(D.Fieldname "field1", 1)]
                                  db <- create_empty_db
                                  _ <- O.create_table db (D.TransactionID "blah" 0) tablename1 (fmap (\(f, d)-> (f, Just(D.Element(Just d)), typeOf(d::Int))) field_and_default1) Nothing
                                  _ <- O.create_table db (D.TransactionID "blah" 0) tablename2 (fmap (\(f, d)-> (f, Just(D.Element(Just d)), typeOf(d::Int))) field_and_default2) Nothing
                                  res1 <- O.show_tables db
                                  _ <- O.drop_table db (D.TransactionID "blah" 0) tablename1
                                  res2 <- O.show_tables db
                                  return $ unlines ["Create 2 tables: ", res1, "Drop first table: ", res2] 

--This test creates a table and adds a column to it.
--This should return the schema as a string.
test_alter_add :: IO String
test_alter_add = atomically $ do let tablename  = D.Tablename "sample_table"
                                 let field_and_default =  [(D.Fieldname "field1", 1), (D.Fieldname "field2", 2)]
                                 db <- create_empty_db
                                 _ <- O.create_table db (D.TransactionID "blah" 0) tablename (fmap (\(f, d)-> (f, Just(D.Element(Just d)), typeOf(d::Int))) field_and_default) Nothing
                                 _ <- O.alter_table_add db (D.TransactionID "blah" 0) tablename (D.Fieldname "field3") (typeOf(undefined::String)) Nothing True
                                 O.show_table_contents db tablename

--This test creates a table and drops a column from it.
--This should return the schema as a string.
test_alter_drop :: IO String
test_alter_drop = atomically $ do let tablename  = D.Tablename "sample_table"
                                  let field_and_default =  [(D.Fieldname "field1", 1), (D.Fieldname "field2", 2)]
                                  db <- create_empty_db
                                  _ <- O.create_table db (D.TransactionID "blah" 0) tablename (fmap (\(f, d)-> (f, Just(D.Element(Just d)), typeOf(d::Int))) field_and_default) Nothing
                                  _ <- O.alter_table_drop db (D.TransactionID "blah" 0) tablename (D.Fieldname "field2")
                                  O.show_table_contents db tablename

--This test creates a table and inserts a row into it.
--This should return the entire table as a string.
test_insert :: IO String 
test_insert = atomically $ do let tablename  = D.Tablename "sample_table"
                              let field_and_default =  [(D.Fieldname "field1", 1), (D.Fieldname "field2", 2)]
                              db <- create_empty_db
                              _ <- O.create_table db (D.TransactionID "blah" 0) tablename (fmap (\(f, d)-> (f, Just(D.Element(Just d)), typeOf(d::Int))) field_and_default) Nothing
                              res <- O.insert db (D.TransactionID "blah" 0) tablename (DU.construct_row [(D.Fieldname "field1", D.Element (Just 5::Maybe Int)), (D.Fieldname "field2", D.Element (Just 6::Maybe Int))])
                              case res of 
                              	Left (D.ErrString str) -> return $ "error: " ++ str
                                _ -> O.show_table_contents db tablename

--This test creates a a table, inserts 3 rows into it, then selects all the rows where the first element is not 5 (which returns a table).
--This should show the table that select returns. 
test_select :: IO String 
test_select = atomically $ do let tablename  = D.Tablename "sample_table"
                              let field_and_default =  [(D.Fieldname "field1", 1), (D.Fieldname "field2", 2)]
                              db <- create_empty_db
                              _ <- O.create_table db (D.TransactionID "blah" 0) tablename (fmap (\(f, d)-> (f, Just(D.Element(Just d)), typeOf(d::Int))) field_and_default) Nothing
                              _ <- O.insert db (D.TransactionID "blah" 0) tablename (DU.construct_row [(D.Fieldname "field1", D.Element (Just 5::Maybe Int)), (D.Fieldname "field2", D.Element (Just 6::Maybe Int))])
                              _ <- O.insert db (D.TransactionID "blah" 0) tablename (DU.construct_row [(D.Fieldname "field1", D.Element (Just 7::Maybe Int)), (D.Fieldname "field2", D.Element (Just 8::Maybe Int))])
                              _ <- O.insert db (D.TransactionID "blah" 0) tablename (DU.construct_row [(D.Fieldname "field1", D.Element (Just 10::Maybe Int)), (D.Fieldname "field2", D.Element (Just 10::Maybe Int))])
                              let func = DU.verify_row [(D.Fieldname "field1", cond)] 
                              res <- O.select db tablename [D.Fieldname "field1"] func      
                              case res of 
                              	Left (D.ErrString str)-> return str
                              	Right t -> O.show_table_contents_helper t    
                           where cond (D.Element e) = case e of 
	 										            Just x -> if show(x) == "5" then False else True
	 										            Nothing -> False 

--This test creates a a table, inserts 3 rows into it, then deletes all the rows where the first element is.
--This should show the table after those operations.
test_delete :: IO String 
test_delete = atomically $ do let tablename  = D.Tablename "sample_table"
                              let field_and_default =  [(D.Fieldname "field1", 1), (D.Fieldname "field2", 2)]
                              db <- create_empty_db
                              _ <- O.create_table db (D.TransactionID "blah" 0) tablename (fmap (\(f, d)-> (f, Just(D.Element(Just d)), typeOf(d::Int))) field_and_default) Nothing
                              _ <- O.insert db (D.TransactionID "blah" 0) tablename (DU.construct_row [(D.Fieldname "field1", D.Element (Just 5::Maybe Int)), (D.Fieldname "field2", D.Element (Just 6::Maybe Int))])
                              _ <- O.insert db (D.TransactionID "blah" 0) tablename (DU.construct_row [(D.Fieldname "field1", D.Element (Just 7::Maybe Int)), (D.Fieldname "field2", D.Element (Just 8::Maybe Int))])
                              _ <- O.insert db (D.TransactionID "blah" 0) tablename (DU.construct_row [(D.Fieldname "field1", D.Element (Just 10::Maybe Int)), (D.Fieldname "field2", D.Element (Just 10::Maybe Int))])
                              let func = DU.verify_row [(D.Fieldname "field1", cond)] 
                              _ <- O.delete db (D.TransactionID "blah" 0) tablename func 
                              O.show_table_contents db tablename
                           where cond (D.Element e) = case e of 
	 										            Just x -> if show(x) == "5" then True else False
	 										            Nothing -> False   

--This test creates a a table, inserts 3 rows into it, then updates all the rows where the first element is 5 by changing the element in the second col to 100.
--This should show the table that select returns. 
test_update :: IO String 
test_update = atomically $ do let tablename  = D.Tablename "sample_table"
                              let field_and_default =  [(D.Fieldname "field1", 1), (D.Fieldname "field2", 2)]
                              db <- create_empty_db
                              _ <- O.create_table db (D.TransactionID "blah" 0) tablename (fmap (\(f, d)-> (f, Just(D.Element(Just d)), typeOf(d::Int))) field_and_default) Nothing
                              _ <- O.insert db (D.TransactionID "blah" 0) tablename (DU.construct_row [(D.Fieldname "field1", D.Element (Just 5::Maybe Int)), (D.Fieldname "field2", D.Element (Just 6::Maybe Int))])
                              _ <- O.insert db (D.TransactionID "blah" 0) tablename (DU.construct_row [(D.Fieldname "field1", D.Element (Just 7::Maybe Int)), (D.Fieldname "field2", D.Element (Just 8::Maybe Int))])
                              _ <- O.insert db (D.TransactionID "blah" 0) tablename (DU.construct_row [(D.Fieldname "field1", D.Element (Just 10::Maybe Int)), (D.Fieldname "field2", D.Element (Just 10::Maybe Int))])
                              let func = DU.verify_row [(D.Fieldname "field1", cond)] 
                              let update = DU.transform_row [(D.Fieldname "field2", transform)] 
                              _ <- O.update db (D.TransactionID "blah" 0) tablename func update 
                              O.show_table_contents db tablename
                           where 
                           transform _ = D.Element(Just 100::Maybe Int) 
                           cond (D.Element e) = case e of 
	 										      Just x -> if show(x) == "5" then False else True
	 										      Nothing -> False  

--This test creates a a table, inserts some rows into it, and calls the checkpointing function, which should write this to disk to a file in .hidb.
--Note that you'll have to kill this executable if you run this test because run_checkpoint does not return.
test_write_and_hydrate :: IO String 
test_write_and_hydrate = do let tablename1  = D.Tablename "sample_table1"
                            (hdb, l, at) <- atomically $ do let field_and_default1 =  [(D.Fieldname "table1_field1", 1), (D.Fieldname "table1_field2", 2)]
                                                            db <- create_empty_db
                                                            l <- newTChan::STM(TChan D.LogOperation)
                                                            at <- newTVar (S.empty)
                                                            should_be_logOps <- O.create_table db (D.TransactionID "blah" 0) tablename1 (fmap (\(f, d)-> (f, Just(D.Element(Just d)), typeOf(d::Int))) field_and_default1) Nothing
                                                            case should_be_logOps of 
                                                              Left _ -> do return (db, l, at) -- should not happen
                                                              Right logOps -> do _ <- mapM (writeTChan l) logOps 
                                                                                 should_be_logOps2 <- populateTable db tablename1 0 5
                                                                                 case should_be_logOps2 of 
                                                                                   Left _ -> do return (db, l, at) -- should not happen
                                                                                   Right lOps -> do _ <- mapM (writeTChan l) lOps 
                                                                                                    return (db, l, at) 
                            DM.run_checkpoint hdb l at 
                            return ""

--This test creates a a table, inserts some rows into it, writes the table to disk, and then calls hydrate to build up the in-memory data structure again.
--This should return a string with the contents of the in-memory table 
test_write_and_hydrate_more :: IO String
test_write_and_hydrate_more = do let tablename1  = D.Tablename "sample_table1"
                                 hdb <- atomically $ do let field_and_default1 =  [(D.Fieldname "table1_field1", 1::Int), (D.Fieldname "table1_field2", 2::Int)]
                                                        db <- create_empty_db
                                                        should_be_logOps <- O.create_table db (D.TransactionID "blah" 0) tablename1 (fmap (\(f, d)-> (f, Just(D.Element(Just d)), typeOf(d::Int))) field_and_default1) Nothing
                                                        case should_be_logOps of 
                                                          Left _ -> do edb <- readTVar db
                                                                       return edb -- should not happen
                                                          Right _ -> do should_be_logOps2 <- populateTable db tablename1 0 5
                                                                        case should_be_logOps2 of 
                                                                          Left _ -> do edb <- readTVar db
                                                                                       return edb-- should not happen
                                                                          Right _ -> do edb <- readTVar db 
                                                                                        return edb
                                 
                                 DM.write_db hdb 
                                 tvar_db <- DM.hydrate
                                 atomically $ O.show_table_contents tvar_db tablename1

-- this will populate the table's "table1_field1" column with increasing integers 1, 2, 3, etc. and "table1_field2" column with 1s.
populateTable :: TVar D.Database -> D.Tablename -> Int -> Int -> STM(Either D.ErrString [D.LogOperation]) 
populateTable _ _ _ 0 = return $ Right []                   
populateTable db tablename tr_id numRows = do res <- O.insert db (D.TransactionID "blah" tr_id) tablename (DU.construct_row [(D.Fieldname "table1_field1", D.Element (Just numRows)), (D.Fieldname "table1_field2", D.Element (Just 1::Maybe Int))])
                                              case res of 
                                                Left errstr -> return $ Left $ errstr
                                                Right logOp -> do res_rest <- populateTable db tablename tr_id (numRows - 1)
                                                                  case res_rest of
                                                                    Right logOps -> return $ Right $ logOp:logOps
                                                                    _ -> return $ res_rest
