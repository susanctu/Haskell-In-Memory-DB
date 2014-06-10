-- A module that _is_ exported as a library

module Client (
	setup,
	addOperation,
	runTransaction
) where

import Network
import System.IO

-- Protocol: send a bunch of operations, one per line
-- then, send "STOP" to end the transaction.
-- "QUIT" disconnects in general.

setup :: HostName -> PortNumber -> IO Handle
setup hostname port = do
	h <- connectTo hostname (PortNumber port)
	hSetBuffering h LineBuffering
	return h

-- tell the database to do another thing (reword better later)
addOperation :: [String] -> String -> [String]
addOperation ops newOp = ops ++ [newOp]

handleShowing :: Handle -> IO ()
handleShowing h = do
	next <- hGetLine h
	case next of
		"DONE" -> return ()
		_	   -> do hPutStrLn stdout next
					 handleShowing h

-- actually execute stuff, and return the result to us.
runTransaction :: Handle -> [String] -> IO ()
runTransaction h cmds = do
	mapM_ (hPutStrLn h) cmds
	hPutStrLn h "STOP" -- signals end of commands
	next <- hGetLine h
	case next of
		"SHOWING" -> handleShowing h
		_		  -> hPutStrLn stdout next
