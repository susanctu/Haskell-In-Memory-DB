import DBTypes
import Control.Concurrent
import Control.Concurrent.Chan
import Control.Concurrent.STM
import Control.Monad
import qualified Data.Map.Lazy as Map
import qualified Data.Set as Set
import qualified Operation as O

write_db :: Database -> IO ()
write_db db = return ()
--write_all $ Map.keys $ database db
--              where write_all tablenames = mapM_ (write_table db) tablenames

flush_log :: Log -> IO ()
flush_log l = return ()

consume_log :: Log -> ActiveTransactions -> IO ()
consume_log l active = do flag <- should_consume l active
                          if flag
                            then readChan l >> consume_log l active
                            else return ()

  where should_consume l active = do l_copy <- dupChan l
                                     op <- readChan l_copy
                                     case op of Start trans_id -> return $ Set.notMember trans_id active
                                                _              -> return True

run_checkpoint :: TVar Database -> Log -> TVar ActiveTransactions -> IO ()
run_checkpoint db l active = do threadDelay 30000000 --thirty seconds
                                flush_log l
                                (unwrapped_db, unwrapped_active) <- atomically $ do a <- readTVar db
                                                                                    b <- readTVar active
                                                                                    return (a,b)
                                checkpoint unwrapped_db l unwrapped_active
                                consume_log l unwrapped_active
                                flush_log l
                                run_checkpoint db l active
    where checkpoint db l active = do
            writeChan l $ StartCheckpoint $ Set.toList active
            write_db $ db
            writeChan l EndCheckpoint

undo :: TVar Database -> [LogOperation] -> IO (Set.Set TransactionID)
undo db ops = foldr (process db) (return Set.empty) ops
    where process db op committed | Commit trans_id <- op = committed >>= return . (Set.insert trans_id)
                                  | Insert trans_id (table, row_hash) content <- op = do unwrapped <- committed 
                                                                                         when (Set.notMember trans_id unwrapped) $ atomically.void $
                                                                                           O.delete db trans_id table $ verify_row $ map (\(fnm, e) -> (fnm, (==) e)) content
                                                                                         return unwrapped
                                  | Delete trans_id (table, row_hash) content <- op = do unwrapped <- committed 
                                                                                         when (Set.notMember trans_id unwrapped) $ atomically.void $
                                                                                           O.insert db trans_id table $ construct_row content
                                                                                         return unwrapped
                                  | Update trans_id (table, row_hash) content <- op = do unwrapped <- committed 
                                                                                         when (Set.notMember trans_id unwrapped) $ atomically.void $
                                                                                           O.update db trans_id table (verify_row $ map (\(fnm, old, new) -> (fnm, (==) new)) content) $
                                                                                             transform_row $ map (\(fnm, old, new) -> (fnm, \e' -> old)) content
                                                                                         return unwrapped
                                  | otherwise = committed
                 
redo :: TVar Database -> [LogOperation] -> Set.Set (TransactionID) -> IO ()
redo db ops committed = sequence_ $ map (process db committed) ops
    where process db committed op | Insert trans_id (table, row_hash) content <- op = when (Set.member trans_id committed) $ atomically . void $
                                                                                          O.insert db trans_id table $ construct_row content
                                  | Delete trans_id (table, row_hash) content <- op = when (Set.member trans_id committed) $ atomically . void $
                                                                                          O.delete db trans_id table $ verify_row $ map (\(fnm, e) -> (fnm, (==) e)) content
                                  | Update trans_id (table, row_hash) content <- op = when (Set.member trans_id committed) $ atomically . void $
                                                                                          O.update db trans_id table (verify_row $ map (\(fnm, old, new) -> (fnm, (==) old)) content) $
                                                                                              transform_row $ map (\(fnm, old, new) -> (fnm, \e' -> new)) content
                                  | otherwise = return ()

recover :: TVar Database -> [LogOperation] -> IO ()
recover db ops = do committed <- undo db ops
                    redo db ops committed

read_db :: IO (TVar Database)
read_db = newTVarIO $ Database Map.empty

read_log :: IO [LogOperation]
read_log = return []

hydrate :: IO (TVar Database)
hydrate = do db <- read_db
             l <- read_log
             recover db l
             return db

start_db :: IO ()
start_db = do db <- hydrate
              l <- newChan :: IO Log
              active <- newTVarIO (Set.empty :: ActiveTransactions)
              --start server with forkIO
              forkIO $ run_checkpoint db l active
              return ()

main = start_db
