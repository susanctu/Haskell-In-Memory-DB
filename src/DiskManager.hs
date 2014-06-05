import DBTypes
import DBUtils
import Control.Concurrent
import Control.Concurrent.STM
import Control.Concurrent.STM.TChan
import Control.Monad
import qualified Data.Map.Lazy as Map
import qualified Data.Set as Set
import qualified Operation as O
import System.Directory
import System.FilePath
import Server (runServer)

special_folder :: FilePath
special_folder = ".hmdb"

log_file :: FilePath
log_file = special_folder </> ".log"

write_db :: Database -> IO ()
write_db db = return ()

flush_log :: Log -> IO ()
flush_log l = do l_copy <- atomically $ dupTChan l
                 l_list <- extract l_copy []
                 writeFile log_file $ show l_list
    where extract chan l = do flag <- atomically $ isEmptyTChan chan
                              if flag then return l
                                      else do li <- atomically $ readTChan chan 
                                              extract chan (li : l)

consume_log :: Log -> ActiveTransactions -> IO ()
consume_log l active = do flag <- should_consume l active
                          if flag
                            then (atomically $ readTChan l) >> consume_log l active
                            else return ()

    where should_consume l active = do l_copy <- atomically $ dupTChan l
                                       op <- atomically $ readTChan l_copy
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
            atomically $ writeTChan l $ StartCheckpoint $ Set.toList active
            write_db $ db
            atomically $ writeTChan l EndCheckpoint

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
read_log = liftM read $ readFile log_file

hydrate :: IO (TVar Database)
hydrate = do createDirectoryIfMissing False special_folder
             db <- read_db
             l <- read_log
             recover db l
             return db

start_db :: IO ()
start_db = do db <- hydrate
              l <- newTChanIO :: IO Log
              active <- newTVarIO (Set.empty :: ActiveTransactions)
              --forkIO $ run_server
              forkIO $ run_checkpoint db l active
              return ()

main = start_db