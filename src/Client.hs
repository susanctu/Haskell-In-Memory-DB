-- A module that _is_ exported as a library

module Client (
	setup,
	addOperation,
	startTransaction
) where

--import Types
import Network
import System.IO

-- Protocol: send a bunch of operations, one per line
-- then, send "STOP" to end the transaction.
-- "QUIT" disconnects in general.

setup :: String -> HostName -> PortNumber -> IO Handle
setup username hostname port = do
	h <- connect hostname port
	hSetBuffering h LineBuffering
	return h

-- tell the database to do another thing (reword better later)
addOperation :: [String] -> String -> [String]
addOperation ops newOp = ops ++ newOps

-- actually execute stuff, and return the result to us.
startTransaction :: Handle -> [String] -> IO [String]
startTransaction h = do
	MapM_ hPutStrLn cmds
	hPutStrLn h "STOP" -- signals end of commands
	hGetLine h >>= hPutStrLn stdout
