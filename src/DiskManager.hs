import DBTypes
import Control.Concurrent
import Control.Concurrent.Chan
import Control.Concurrent.STM
import Control.Monad
import qualified Data.Map.Lazy as Map
import qualified Data.Set as Set

write_db :: Database -> IO ()
--write_all $ Map.keys $ database db
--	            where write_all tablenames = mapM_ (write_table db) tablenames

flush_log :: Log -> IO ()

consume_log :: Log -> ActiveTransactions -> IO ()
consume_log l active = do flag <- should_consume l active
	                      if flag
	                      	then readChan l >> consume_log l active
	                      	else return ()

	where should_consume l active = do l_copy <- dupChan l
	                                   op <- readChan l_copy
	                                   case op of Start trans_id -> return Set.notMember trans_id active
	                                              _              -> return True

run_checkpoint :: TVar Database -> Log -> TVar ActiveTransactions -> IO ()
run_checkpoint db l active = do threadDelay 30000000 --thirty seconds
                                flush_log l
                                unwrapped_db <- readTVarIO db
                                unwrapped_active <- readTVarIO active -- TODO: readTVarIOs actually should be done transactionally
                                checkpoint unwrapped_db l unwrapped_active
                                consume_log l unwrapped_active
                                flush_log l
                                run_checkpoint db l active
    where checkpoint db l active = do
	          writeChan l $ StartCheckpoint $ toList _active
	          write_db $ database db
	          writeChan l EndCheckpoint

undo :: TVar Database -> [LogOperations] -> IO (Set TransactionID)
undo db ops = foldr (process db) (return Set.empty) ops
    where process db op committed | Commit trans_id <- op = committed >>= return $ Set.insert trans_id
                                  | TransactionLog trans_id (table, field, row) old_val new_val = do
                                  	    
                                  | otherwise = committed
                 


redo :: TVar Database -> [LogOperations] -> Set (TransactionID) -> IO ()

recover :: TVar Database -> [LogOperations] -> IO ()
recover db ops = committed <- undo db ops
                 redo db ops committed

read_db :: IO (TVar Database)

read_log :: IO [LogOperations]

hydrate :: IO (TVar Database)
hydrate = do db <- read_db
             l <- read_log
             recover db l
             return db

start_db :: IO ()
start_db = do db <- hydrate
              l <- newChan :: Log
              active <- newTVarIO (Set.empty :: ActiveTransactions)
              --start server with forkIO
              forkIO $ run_checkpoint db l active
