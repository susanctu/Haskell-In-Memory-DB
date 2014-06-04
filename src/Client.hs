-- A module that _is_ exported as a library

module Client (
	setup,
	addOperation,
	startTransaction
) where

--import Types
import Network
import System.IO

setup :: String -> HostName -> PortNumber -> IO Handle
setup username hostname port = do
	h <- connect hostname port
	hSetBuffering h LineBuffering
	hPutStrLn h username
	return h

-- tell the database to do another thing (reword better later)
addOperation :: [String] -> String -> [String]
addOperation ops newOp = ops ++ newOps

-- should add some error checking.
pushCmd :: Handle -> String -> IO ()
pushCmd h cmd = do
	hPutStrLn h cmd
	return $ hGetStrLn h

-- actually execute stuff, and return the result to us.
startTransaction :: Handle -> [String] -> IO [String]
startTransaction h = MapM (pushCmd h)
