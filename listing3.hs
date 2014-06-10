{- This function takes care of the logistics behind executing an atomic block
   of actions: updating the transaction set, doing the logging, and so on.

   Since this is called from executeRequests, which parses its list of commands
   into chunks that should be executed atomically, this function assumes that
   its commands should be executed within one atomic block.
 -}

import qualified Data.Set as S

atomicAction :: TVar Database -> TVar ActiveTransactions -> Log -> TransactionID -> [String] -> IO (Maybe ErrString)
atomicAction db transSet logger tID cmds = do
	(toLog, errStr) <- atomically $ do
		modifyTVar transSet (S.insert tID)
		compRes <- commandWrapper db tID cmds []
		modifyTVar transSet (S.delete tID)
		return compRes -- type ([LogOperation], Maybe ErrString) wrapped in IO
	-- then, write to the log, which is a Chan of LogOperations
	mapM_ (atomically . writeTChan logger) toLog
	return errStr

{- to handle error-checking, since it would be a bit awkward within atomicAction
   recurses by passing in the LogOperations done thus far, so it can quit if need be.
   the "maybe" is a nothing if there were no errors.
 -}
commandWrapper :: TVar Database -> TransactionID -> [String] -> [LogOperation] -> STM ([LogOperation], Maybe ErrString)
commandWrapper _ _ [] logVal = return (logVal, Nothing)
commandWrapper db tID cmds prtResults = do
	-- parseCommand actuall determines which command was called and executes it
	queryResult <- parseCommand db tID $ words $ head cmds
	case queryResult of
		Left errStr -> return (prtResults, Just errStr) -- no further computations should be done.
		Right logVal -> do
			commandWrapper db tID (tail cmds) (prtResults ++ logVal)
