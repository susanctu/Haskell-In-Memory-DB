-- This part of the database listens for queries, parses them, and then reports the action or an error

module Server (
	runServer -- perhaps more later.
) where

import Control.Concurrent
import qualified Data.ByteString as B
import Data.List.Split (splitOneOf)
import Data.Int
import System.Random

import Network
import Types -- for all the types of the functions in Operation. Hmmm.
import Operation

-- throws away the logging operations that we don't expose to the user... I think.
-- if I am wrong, this isn't hard to fix.
justLeft :: IO (Either ErrString b) -> IO (Either ErrString String)
justLeft result = do
	case result of
		Left errStr -> return $ Left errStr
		Right _		-> return $ Right "" -- empty string => no error, nothing to return

-- The following two functions aren't the best designed, but I'm not sure we want to
-- make the distinction.
-- char(n) or varchar(n)
isCharType :: String -> Bool
isCharType s = take 4 s == "char" || take 7 s == "varchar"

-- Haskell doesn't seem to have a native bit-array type, so not clear what we should do.
-- I like the idea of a (relatively) packed array of Bools, though!
-- bit(n) or bitvarying(n)
isBitType :: String -> Bool
isBitType s = take 3 s == "bit" -- since there are only so many types

-- True, False, or Unknown... blah.
-- everything else is done just with read...
getBool :: String -> Maybe Bool
getBool "TRUE"	= Just True
getBool "FALSE"	= Just False
getBool _ 		= Nothing

-- generates the default value from the array... or Nothing.
obtainDefault :: [String] -> Maybe String
obtainDefault words
	| length words <  4 = Nothing
	| length words >= 4 = Just $ words !! 3

detectPrimaryKey :: [String] -> Maybe String
detectPrimaryKey = case (filter (\args -> head (words args) == "PRIMARY") fieldInfo) of
	x:_ -> Just $ (words x) !! 2 -- since it's of the form PRIMARY KEY fieldname
	[]	-> Nothing

-- accepts a string of the form "fieldname type" or "fieldname type DEFAULT default_val"
-- we'll handle the default value later
createField :: String -> (Fieldname a, Maybe Element, TypeRep)
createField fieldInfo
	-- can we depend on consistent capitalization (or lack thereof) of type names?
	| ftype == "boolean"	= (name, getBool df, typeOf(undefined :: Maybe Bool))
	{- Do we want there to be new types, new type constructors, etc., for these char and varchar types...?
	   Would be basically wrappers around B.ByteString, with some max. length.

	   isCharType right now will also handle varchars
	 -}
	| isCharType ftype || isBitType ftype = (name, read' df :: B.ByteString, typeOf(undefined :: B.ByteString))
	-- I guess there should be a bitstring type. Derive from ByteString, somehow...?
	| ftype == "integer"	= (name, read' df :: Int32, typeOf(undefined :: Int32))
	| ftype == "real"		= (name, read' df :: Double, typeOf(undefined :: Double))
	where args  = words fieldInfo
		  name  = Fieldname $ args !! 0
		  ftype = args !! 1
		  df	= obtainDefault words
		  read' = liftM read

-- xs is of the form (fieldname type, fieldname type, fieldname type, PRIMARY KEY fieldname)
create_util :: TVar Database -> TransactionID -> String -> String -> IO (Either ErrString String)
-- Note: in general, tableStr means a String holding a Tablename that has yet to be converted.
create_util db tID tableStr xs = do
	let fieldInfo = splitOneOf "(,)" xs -- warning! This will kill parentheses elsewhere in the strings.
		-- split into the primary and non-primary section
		primaryKey = detectPrimaryKey fieldInfo
		fieldTypes = map createField $ filter (\args -> head (words args) /= "PRIMARY") fieldInfo
	result <- atomically $ create_table db tID (Tablename tableStr) fieldTypes primaryKey
	return $ justLeft result

drop_util :: TVar Database -> TransactionID -> String -> IO (Either ErrString String)
drop_util db tID tableStr = justLeft $ atomically $ drop_table db tID (Tablename tableStr)

-- TODO this needs to handle the default value, somehow
-- this is a parsing problem, I believe.
-- also, I need to add the possibility of this being the default key.
alter_add_util :: TVar Database -> TransactionID -> String -> String -> String -> IO (Either ErrString String)
alter_add_util db tID tableStr fieldStr typename = do
	let tablename = Tablename tableStr
		fieldname = Fieldname fieldStr
		typeKind  = readType typename
	result <- atomically $ alter_table_add db tID tablename fieldname typeKind Nothing False
	return $ justLeft result

alter_drop_util :: TVar Database -> TransactionID -> String -> String -> IO (Either ErrString String)
alter_drop_util db tID tableStr fieldStr = do
	let tablename = Tablename tableStr
		fieldname = Fieldname fieldStr
	return $ justLeft $ atomically $ alter_table_drop db tID tablename fieldname

-- This is parsed as INSERT INTO tablename(fieldname) VALUES values
-- I'm pretty sure this needs to be reworked.
insert_util :: TVar Database -> TransactionID -> String -> String -> IO (Either String String)
insert_util db tID tableData values = do
	hash <- getStdRandom random -- this is still an Int; needs to be converted into a RowHash
	atomically $ insert db tID (RowHash rowHash) (TableName tablename) {- values -}
	where (tablename:fieldname:_) = splitOneOf "()" tableData
		  valueList = splitOneOf "(:); " values

{-delete_util :: TVar Database -> TransactionID -> String ->

update_util :: TVar Database -> TransactionID -> String -> IO (Either String String)
update_util db tID toParse
	| toParse !! 1 /= "SET"		= Left "UPDATE clause improperly formed."
	| --- uhhhhhh -}
delete_util = undefined
update_util = undefined

parseCommand :: TVar Database -> TransactionID -> [String] -> IO (Either String String)
	parseCommand db tID ("CREATE":"TABLE":tablename:xs) = create_util db tID tablename $ unwords xs
	parseCommand db tID ["DROP", "TABLE", tablename] = drop_util db tID tablename
	parseCommand db tID ["ALTER", "TABLE", tablename, "ADD", fieldname, typename] = alter_add_util db tID tablename fieldname typename
	parseCommand db tID ["ALTER", "TABLE", tablename, "DROP", fieldname] = atomically $ alter_table_drop db tID tablename fieldname
	-- SELECT ... syntax will be trickier
	parseCommand db tID ["INSERT", "INTO", tableData, "VALUES", values] = insert_util db tID tableData values
	-- parseCommand db tID ["DELETE", "FROM", tablename, "WHERE", conditions] = atomically $ delete db tID tablename {-???-}
	-- I need to figure out what these statements look like. All right.
	-- parseCommand db tID ("UPDATE":xs) = update_util db tID xs
	parseCommand db _ ["SHOW", "TABLES"] = Right $ atomically $ show_tables db
	parseCommand _ _ = Left "Command not found."

-- loops a session with a single client. Runs in its own thread.
-- TODO error-handling.
clientSession :: String -> Int -> TVar Database -> Handle -> IO ()
clientSession name tID db h = do
	cmd <- hGetLine h
	if cmd == "QUIT" then do
		hClose h
		return ()
	else do
		let t = TransactionID {
			clientName = name,
			transactionNum = tID
		}
	  	result <- parseCommand db t $ words cmd
		case result of
			Left error -> hPutStrLn h $ "ERROR: " ++ error
			Right msg -> hPutStrLn h msg
		clientSession name (tID+1) db h

-- TODO: possibly use a ThreadPool
{- The point of initVal might not be clear: different threads must still create
   different transaction IDs, so we space them out by a very large number to prevent
   two threads' IDs from colliding.

   The upshot is that when there are multiple threads, IDs of actions don't necessarily come in order,
   but they will be unique.
 -}
processRequests :: TVar Database -> Int -> Socket -> IO ()
processRequests db initVal s = do
	(h, _, _) <- accept s
	hSetBuffering h LineBuffering
	{- The protocol requires the client to send its name first. This isn't the most normal
	   protocol, but we need a clientName around somewhere and this seems like the best way to
	   get it. I guess I should also check it against a set of already-used names...
	 -}
	name <- hGetStrLn h
	threadID <- forkIO clientSession name initVal db h
	processRequests db s (initVal + 1000000)

-- entry point, assuming initialization of the database.
runServer :: PortNumber -> TVar Database IO ()
runServer port db = do
	-- listen for connections and spin each one off into its own thread.
	bracket (listenOn port) sClose (processRequests db 0)