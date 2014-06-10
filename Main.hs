module Main where

import qualified Operation as O
import qualified DBTypes as D 
import qualified DBUtils as DU
import Data.Maybe
import Control.Concurrent.STM
import qualified Data.Map.Lazy as L 
import Data.Typeable
import Control.Exception
import System.Exit
import DiskManager as DM
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
                                ,test_write_and_hydrate
                                ]
          mapM putStrLn output
          return ()


test_create_table :: IO String
test_create_table = atomically $ do let tablename  = D.Tablename "sample_table"
                                    let field_and_default =  [(D.Fieldname "field1", 1), (D.Fieldname "field2", 2)]
                                    db <- create_empty_db
                                    res <- O.create_table db (D.TransactionID "blah" 0) tablename (fmap (\(f, d)-> (f, Just(D.Element(Just d)), typeOf(d::Int))) field_and_default) Nothing
                                    case res of 
                                      Left (D.ErrString errstr) -> return errstr
                                      Right _ -> O.show_table_contents db tablename

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

test_alter_add :: IO String
test_alter_add = atomically $ do let tablename  = D.Tablename "sample_table"
                                 let field_and_default =  [(D.Fieldname "field1", 1), (D.Fieldname "field2", 2)]
                                 db <- create_empty_db
                                 _ <- O.create_table db (D.TransactionID "blah" 0) tablename (fmap (\(f, d)-> (f, Just(D.Element(Just d)), typeOf(d::Int))) field_and_default) Nothing
                                 _ <- O.alter_table_add db (D.TransactionID "blah" 0) tablename (D.Fieldname "field3") (typeOf(undefined::String)) Nothing True
                                 O.show_table_contents db tablename

test_alter_drop :: IO String
test_alter_drop = atomically $ do let tablename  = D.Tablename "sample_table"
                                  let field_and_default =  [(D.Fieldname "field1", 1), (D.Fieldname "field2", 2)]
                                  db <- create_empty_db
                                  _ <- O.create_table db (D.TransactionID "blah" 0) tablename (fmap (\(f, d)-> (f, Just(D.Element(Just d)), typeOf(d::Int))) field_and_default) Nothing
                                  _ <- O.alter_table_drop db (D.TransactionID "blah" 0) tablename (D.Fieldname "field2")
                                  O.show_table_contents db tablename

test_insert :: IO String 
test_insert = atomically $ do let tablename  = D.Tablename "sample_table"
                              let field_and_default =  [(D.Fieldname "field1", 1), (D.Fieldname "field2", 2)]
                              db <- create_empty_db
                              _ <- O.create_table db (D.TransactionID "blah" 0) tablename (fmap (\(f, d)-> (f, Just(D.Element(Just d)), typeOf(d::Int))) field_and_default) Nothing
                              res <- O.insert db (D.TransactionID "blah" 0) tablename (DU.construct_row [(D.Fieldname "field1", D.Element (Just 5::Maybe Int)), (D.Fieldname "field2", D.Element (Just 6::Maybe Int))])
                              case res of 
                              	Left (D.ErrString str) -> return $ "error: " ++ str
                                _ -> O.show_table_contents db tablename

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
                           where cond (D.Element elem) = case elem of 
	 										              Just x -> if show(x) == "5" then False else True
	 										              Nothing -> False 

test_delete :: IO String 
test_delete = atomically $ do let tablename  = D.Tablename "sample_table"
                              let field_and_default =  [(D.Fieldname "field1", 1), (D.Fieldname "field2", 2)]
                              db <- create_empty_db
                              _ <- O.create_table db (D.TransactionID "blah" 0) tablename (fmap (\(f, d)-> (f, Just(D.Element(Just d)), typeOf(d::Int))) field_and_default) Nothing
                              _ <- O.insert db (D.TransactionID "blah" 0) tablename (DU.construct_row [(D.Fieldname "field1", D.Element (Just 5::Maybe Int)), (D.Fieldname "field2", D.Element (Just 6::Maybe Int))])
                              _ <- O.insert db (D.TransactionID "blah" 0) tablename (DU.construct_row [(D.Fieldname "field1", D.Element (Just 7::Maybe Int)), (D.Fieldname "field2", D.Element (Just 8::Maybe Int))])
                              _ <- O.insert db (D.TransactionID "blah" 0) tablename (DU.construct_row [(D.Fieldname "field1", D.Element (Just 10::Maybe Int)), (D.Fieldname "field2", D.Element (Just 10::Maybe Int))])
                              let func = DU.verify_row [(D.Fieldname "field1", cond)] 
                              O.delete db (D.TransactionID "blah" 0) tablename func 
                              O.show_table_contents db tablename
                           where cond (D.Element elem) = case elem of 
	 										              Just x -> if show(x) == "5" then True else False
	 										              Nothing -> False   

 
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
                           cond (D.Element elem) = case elem of 
	 										         Just x -> if show(x) == "5" then False else True
	 										         Nothing -> False  

test_write_and_hydrate :: IO String 
test_write_and_hydrate = do let tablename1  = D.Tablename "sample_table1"
                            let tablename2 = D.Tablename "sample_table2"
                            sssssssssstr <- atomically $ do let field_and_default1 =  [(D.Fieldname "table1_field1", 1), (D.Fieldname "table1_field2", 2)]
                                                            let field_and_default2 = [(D.Fieldname "table2_field1", 1)]
                                                            db <- create_empty_db
                                                            l <- newTChan::STM(TChan D.LogOperation)
                                                            at <- newTVar (S.empty)
                                                            should_be_logOps <- O.create_table db (D.TransactionID "blah" 0) tablename1 (fmap (\(f, d)-> (f, Just(D.Element(Just d)), typeOf(d::Int))) field_and_default1) Nothing
                                                            case should_be_logOps of 
                                                              Left errstring -> return "" -- should not happen
                                                              Right logOps -> do _ <- mapM (writeTChan l) logOps 
                                                                                 should_be_logOps2 <- populateTable db tablename1 1 0
                                                                                 case should_be_logOps2 of 
                                                                                   Left errstring -> return "" -- should not happen
                                                                                   Right logOps -> do _ <- mapM (writeTChan l) logOps 
                                                                                                      return "success"
                            return sssssssssstr
                            --DM.run_checkpoint hdb l at 
                            --return ""
                            --tvar_db <- hydrate
                            --atomically $ do t1_after <- O.show_table_contents tvar_db tablename1 
                              --              --t2_after <- O.show_table_contents tvar_db tablename2
                                --            return $ t1_after

-- this will populate the table with increasing integers 1, 2, 3, etc.
populateTable :: TVar D.Database -> D.Tablename -> Int -> Int -> STM(Either D.ErrString [D.LogOperation]) 
populateTable db tablename tr_id 0 = return $ Right []                   
populateTable db tablename tr_id numRows = do res <- O.insert db (D.TransactionID "blah" tr_id) tablename (DU.construct_row [(D.Fieldname "field1", D.Element (Just numRows)), (D.Fieldname "field2", D.Element (Just 1::Maybe Int))])
                                              case res of 
                                                Left errstr -> return $ Left $ errstr
                                                Right logOp -> do res_rest <- populateTable db tablename tr_id (numRows - 1)
                                                                  case res_rest of
                                                                    Right logOps -> return $ Right $ logOp:logOps
                                                                    res -> return $ res_rest
                                              


