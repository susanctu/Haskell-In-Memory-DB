-- for the definition of atomicAction, see the next listing.

executeRequests :: TVar Database -> TVar ActiveTransactions -> Log -> TransactionID -> [String] -> IO (Maybe ErrString)
-- base case
executeRequests _ _ _ _ [] = return Nothing
-- altersTable checks whether a command is one of the aforementioned ones
-- that alters the structure of the database.
executeRequests db transSet logger tID cmds = do
	case findIndex altersTable cmds of
		Just 0 -> do -- the first request alters the table.
			result <- actReq [head cmds]
			recurseReq $ tail cmds
		Just n -> do -- the (n+1)st request alters the table, but the first n don't.
			actReq $ take n cmds
			recurseReq $ drop n cmds
		-- if nothing changes the table, then we can just do everything.
		Nothing -> actReq cmds
	where actReq = atomicAction	db transSet logger tID 
		  recurseReq = executeRequests db transSet logger (incrementTId tID)
