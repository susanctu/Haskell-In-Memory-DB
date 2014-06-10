-- This part of the database listens for queries, parses them, and then reports the action or an error

module Server (
    runServer -- perhaps more later.
) where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception (bracket)
import Control.Monad

import qualified Data.ByteString as B
import Data.List (findIndex)
import Data.List.Split (splitOneOf)
import qualified Data.Set as S
import qualified Data.Map as M
import Data.Typeable

--import System.Random
import System.IO

import Network
import DBTypes -- for all the types of the functions in Operation. Hmmm.
import DBUtils
import Operation

type QueryResult = Either ErrString [LogOperation]

{- The following two functions aren't the best designed, but I'm not sure we want to
-- make the distinction.
-- char(n) or varchar(n)
isCharType :: String -> Bool
isCharType s = take 4 s == "char" || take 7 s == "varchar"

-- Haskell doesn't seem to have a native bit-array type, so not clear what we should do.
-- I like the idea of a (relatively) packed array of Bools, though!
-- bit(n) or bitvarying(n)
isBitType :: String -> Bool
isBitType s = take 3 s == "bit" -- since there are only so many types
-}

-- True, False, or Unknown... blah.
-- everything else is done just with read...
{-getBool :: String -> Maybe Bool
getBool "TRUE"    = Just True
getBool "FALSE"   = Just False
getBool _         = Nothing -}

-- generates the default value from the array... or Nothing.
obtainDefault :: [String] -> Maybe String
obtainDefault xs
    | length xs <  4 = Nothing
    | length xs >= 4 = Just $ xs !! 3
    | otherwise      = Nothing

detectPrimaryKey :: [String] -> Maybe Fieldname
detectPrimaryKey fieldInfo = case (filter (\args -> head (words args) == "PRIMARY") fieldInfo) of
    x:_ -> Just $ Fieldname $ (words x) !! 2 -- since it's of the form PRIMARY KEY fieldname
    []    -> Nothing

-- why was this commented out...?
readType :: String -> TypeRep
readType ftype
    | ftype == "boolean" = typeOf(undefined :: Bool)
    | isCharType ftype || isBitType ftype = typeOf(undefined :: B.ByteString)
    | ftype == "integer" = typeOf(undefined :: Int)
    | ftype == "real"    = typeOf(undefined :: Double)
    | otherwise = typeOf(undefined :: B.ByteString)

--inelegant, but sort-of polymorphism.
getBool :: Maybe String -> Maybe Bool
getBool Nothing = Nothing
getBool (Just s) = Just (read s :: Bool)

getByteString :: Maybe String -> Maybe B.ByteString
getByteString = liftM (\s -> read s :: B.ByteString)

getInt :: Maybe String -> Maybe Int
getInt = liftM (\s -> read s :: Int)

getDouble :: Maybe String -> Maybe Double
getDouble = liftM (\s -> read s :: Double)

-- since there are too many monads floating around right now.
setupDefault :: (Ord a, Read a, Show a, Typeable a) => Maybe a -> Maybe Element
setupDefault Nothing = Nothing
setupDefault (Just x) = Just $ Element $ Just x

-- accepts a string of the form "fieldname type" or "fieldname type DEFAULT default_val"
-- we'll handle the default value later
createField :: String -> (Fieldname, Maybe Element, TypeRep)
createField fieldInfo
    -- can we depend on consistent capitalization (or lack thereof) of type names?
    | ftype == "boolean"    = (name, setupDefault (getBool df), typeOf(undefined :: Bool))
    {- Do we want there to be new types, new type constructors, etc., for these char and varchar types...?
       Would be basically wrappers around B.ByteString, with some max. length.

       isCharType right now will also handle varchars
     -}
    | isCharType ftype || isBitType ftype = (name, setupDefault (getByteString df), typeOf(undefined :: B.ByteString))
    -- I guess there should be a bitstring type. Derive from ByteString, somehow...?
    | ftype == "integer"    = (name, setupDefault (getInt df), typeOf(undefined :: Int))
    | ftype == "real"       = (name, setupDefault (getDouble df), typeOf(undefined :: Double))
    -- do we care enough to make this the default?
    | otherwise             = (name, setupDefault (getByteString df), typeOf(undefined :: B.ByteString))
    where args  = words fieldInfo
          name  = Fieldname $ args !! 0
          ftype = args !! 1
          df    = obtainDefault args

-- xs is of the form (fieldname type, fieldname type, fieldname type, PRIMARY KEY fieldname)
create_util :: TVar Database -> TransactionID -> String -> String -> STM QueryResult
-- Note: in general, tableStr means a String holding a Tablename that has yet to be converted.
create_util db tID tableStr xs = do
    let fieldInfo = splitOneOf "(,)" xs -- warning! This will kill parentheses elsewhere in the strings.
        -- split into the primary and non-primary section
        pk = detectPrimaryKey fieldInfo
        fieldTypes = map createField $ filter (\args -> head (words args) /= "PRIMARY") fieldInfo
    create_table db tID (Tablename tableStr) fieldTypes pk

-- TODO this needs to handle the default value, somehow
-- this is a parsing problem, I believe.
-- also, I need to add the possibility of this being the default key.
alter_add_util :: TVar Database -> TransactionID -> String -> String -> String -> STM QueryResult
alter_add_util db tID tableStr fieldStr typename = do
    let tablename = Tablename tableStr
        fieldname = Fieldname fieldStr
        typeKind  = readType typename
    alter_table_add db tID tablename fieldname typeKind Nothing False

alter_drop_util :: TVar Database -> TransactionID -> String -> String -> STM QueryResult
alter_drop_util db tID tableStr fieldStr = do
    let tablename = Tablename tableStr
        fieldname = Fieldname fieldStr
    alter_table_drop db tID tablename fieldname

split :: String -> [String]
split cond = let split_delim delim strs = concat $ map (split_str_delim delim) strs
               in split_delim "!=<>" $ split_delim "()" $ words cond
    where split_str_delim :: String -> String -> [String]
          split_str_delim delim [] = []
          split_str_delim delim str = (\(a,b) -> (a : split_str_delim delim b)) $
                                        span (\a -> elem a delim == elem (head str) delim) str

check_parens :: [String] -> Bool
check_parens elems = (count elems 0) == 0
    where count [] n     = n
          count (x:xs) n = let increment_count c n | n < 0     = n
                                                   | c == '('  = n+1
                                                   | c == ')'  = n-1
                                                   | otherwise = n
                             in count xs $ foldr increment_count n x

-- interpret the right side of an atomic condition as a string,
-- an integer, or a fieldname.
isConstantConstraint :: String -> Bool
isConstantConstraint s
	| '0' <= head s && head s <= '9' = True
	| head s == '"'					 = True
	| head s == '\''				 = True
	| otherwise						 = False

-- difficulty here is reading the type without access to the table...
{-createOp :: String -> String -> String -> M.Map Fieldname (Element -> Bool)
createOp op left right = singleton left fn
	where fn a = case op of
    ">"  -> a > 
    "<"  ->
    ">=" ->
    "<=" ->
    "==" ->
	-- only work on strings, but we can tell the type...
    "in" ->
    "contains" -> -}
createOp = undefined

transform :: [String] -> [Either String (M.Map Fieldname (Element -> Bool))]
transform []        = []
transform (e:es) = process e ++ transform es
    where process str | null str                           = []
                      | str == "and" || str == "or"        = [Left str]
                      | head str == '(' || head str == ')' = (Left [head str] : process (tail str))
                      | (left : op : right : rem) <-  str  = (Right (createOp op left right) : process rem)
                      | otherwise                          = []

-- utility needed for findIndex, below
leftEq :: String -> Either String a -> Bool
leftEq _ (Right _) = False
leftEq s (Left t)  = s == t

findParens :: [Either String (M.Map Fieldname (Element -> Bool))] -> Maybe (Int, Int)
findParens es = do
	firstLeft <- findIndex (leftEq "(") es
	nextRight <- (liftM2 (\a b -> a + b)) (Just $ 1+firstLeft) $ findIndex (leftEq ")") (drop (1+firstLeft) es)
	prevLeft <- (liftM2 (\a b -> a - b)) (Just $ length es) $ findIndex (leftEq"(") (drop (length es - nextRight) $ reverse es)
	return (prevLeft, nextRight)

-- need to replace elem with something that only compares on the left.
merge :: [Either String (M.Map Fieldname (Element -> Bool))] -> M.Map Fieldname (Element -> Bool)
merge es | elem (Left "(") es   =
        case findParens es of
			Just (start, end) ->
				merge $ (take start es) ++ ([Right $ merge (drop (start+1) (take (end) es))]) ++ (drop (end+1) es)
			Nothing -> M.empty
         | elem (Left "and") es = undefined
         | elem (Left "or") es  = undefined
         | null es              = M.empty
         | (Right e:[]) <- es   = e

parse_predicate :: String -> Row -> STM Bool
parse_predicate cond = let elems = split cond
                         in if check_parens elems then verify_row $ M.toList $ merge $ transform elems
                                                  else (\_ -> do return False)
-- This is parsed as INSERT INTO tablename(fieldname) VALUES values
-- I'm pretty sure this needs to be reworked.
{-insert_util :: TVar Database -> TransactionID -> String -> String -> STM QueryResult
insert_util db tID tableData values = do
    hash <- getStdRandom random -- this is still an Int; needs to be converted into a RowHash
    insert db tID (RowHash rowHash) (TableName tablename) {- values -}
    where (tablename:fieldname:_) = splitOneOf "()" tableData
          valueList = splitOneOf "(:); " values
-}
--delete_util = undefined
--update_util = undefined

parseCommand :: TVar Database -> TransactionID -> [String] -> STM QueryResult
parseCommand db tID ("CREATE":"TABLE":tablename:xs) = create_util db tID tablename $ unwords xs
parseCommand db tID ["DROP", "TABLE", tablename] = drop_table db tID (Tablename tablename)
parseCommand db tID ["ALTER", "TABLE", tablename, "ADD", fieldname, typename] = alter_add_util db tID tablename fieldname typename
parseCommand db tID ["ALTER", "TABLE", tablename, "DROP", fieldname] = alter_drop_util db tID tablename fieldname

{- TODOs:
    SELECT
    INSERT INTO
    DELETE
    UPDATE -}

    -- show_tables is handled earlier.
parseCommand _ _ _ = return $ Left $ ErrString "Command not found."
-- below this waterline, haven't updated my stuff.
        -- SELECT ... syntax will be trickier
    --parseCommand db tID ["INSERT", "INTO", tableData, "VALUES", values] = insert_util db tID tableData values
    -- parseCommand db tID ["DELETE", "FROM", tablename, "WHERE", conditions] = atomically $ delete db tID tablename {-???-}
    -- parseCommand db tID ("UPDATE":xs) = update_util db tID xs


-- to handle error-checking, since it would be a bit awkward within atomicAction
-- recurses by passing in the LogOperations done thus far, so it can quit if need be.
-- the "maybe" is a nothing if there were no errors.
commandWrapper :: TVar Database -> TransactionID -> [String] -> [LogOperation]
                                -> STM ([LogOperation], Maybe ErrString)
commandWrapper _ _ [] logVal = return (logVal, Nothing)
commandWrapper db tID cmds prtResults = do
    queryResult <- parseCommand db tID $ words $ head cmds
    case queryResult of
        Left errStr -> return (prtResults, Just errStr) -- no further computations should be done.
        Right logVal -> do
            commandWrapper db tID (tail cmds) (prtResults ++ logVal)

-- "atomic action" sounds like the name of an environmental protest group.
{- This function takes care of the logistics behind executing an atomic block
   of actions: updating the transaction set, doing the logging, and so on.

   The type signature is identical to executeRequests, below, but this time
   the list of commands has the correct atomicity.
 -}
atomicAction :: TVar Database -> TVar ActiveTransactions -> Log ->
                TransactionID -> [String] -> IO (Maybe ErrString)
atomicAction db transSet logger tID cmds = do
    (toLog, errStr) <- atomically $ do
        modifyTVar transSet (S.insert tID)
        compRes <- commandWrapper db tID cmds []
        modifyTVar transSet (S.delete tID)
        return compRes -- type ([LogOperation], Maybe ErrString) wrapped in IO
    -- then, write to the log, which is a Chan of LogOperations
    mapM_ (atomically . writeTChan logger) toLog
    return errStr

-- increments the Transaction ID by 1, leaving its name the same.
incrementTId :: TransactionID -> TransactionID
incrementTId tID = TransactionID {
    clientName = clientName tID,
    transactionNum = 1 + transactionNum tID
}

-- TODO: determine whether a given command will alter the table,
-- thus forcing it to be given its own atomic command.
altersTable :: String -> Bool
altersTable s =
	case words s of
		("CREATE":"TABLE":_) -> True
		("DROP":"TABLE":_)   -> True
		("ALTER":"TABLE":_)  -> True
		_					 -> False

{- Split off recursively: all requests that don't modify the table should be done atomically.
   Those that do modify the table should be done in their own call to atomically.

   Thus, this function groups the first transactions that don't modify the table and runs them,
   then recurses.
 -}
executeRequests :: TVar Database -> TVar ActiveTransactions -> Log ->
                   TransactionID -> [String] -> IO (Maybe ErrString)
-- base case
executeRequests _ _ _ _ [] = return Nothing
executeRequests db transSet logger tID cmds = do
    case findIndex altersTable cmds of
        Just 0 -> do -- the first request alters the table.
        -- hmm do I care that these are being thrown away?
            result <- actReq [head cmds] -- agg. Then what?
            recurseReq $ tail cmds
        Just n -> do -- the (n+1)st request alters the table, but the first n don't.
            actReq $ take n cmds
            recurseReq $ drop n cmds
        -- if nothing changes the table, then I can just do everything.
        Nothing -> actReq cmds
    where actReq     = atomicAction    db transSet logger  tID
          recurseReq = executeRequests db transSet logger (incrementTId tID)

readCmds :: Handle -> [String] -> IO [String]
readCmds h arr = do
    s <- hGetLine h
    if s == "STOP"
    then return arr
    else readCmds h $ arr ++ [s]

-- loops a session with a single client. Runs in its own thread.
-- TODO error-handling.
clientSession :: TVar Database -> TVar ActiveTransactions -> Log ->
                 TransactionID -> Handle -> String -> IO ()
clientSession db transSet logger tID h name = do
    cmds <- readCmds h []
    case (head cmds) of
        "QUIT" -> do
            hClose h
            return ()
--        "SHOW TABLES" -> hPutStrLn $ atomically $ show_table db
--        will not be only one line. Have a protocol.
--         need to put this somewhere...
        _ -> do
            result <- executeRequests db transSet logger tID cmds
            case result of
                Just (ErrString err) -> hPutStrLn h $ "ERROR: " ++ err
                Nothing -> return () -- nothing needs to be done.
            clientSession db transSet logger (incrementTId tID) h name

-- TODO: possibly use a ThreadPool
{- The point of initVal might not be clear: different threads must still create
   different transaction IDs, so we space them out by a very large number to prevent
   two threads' IDs from colliding.

   The upshot is that when there are multiple threads, IDs of actions don't necessarily come in order,
   but they will be unique.
 -}
processRequests :: TVar Database -> TVar ActiveTransactions -> Log ->
                   Int -> Socket -> IO ()
processRequests db transSet logger initVal s = do
    -- hostName is useful for assigning a name to each client.
    (h, hostName, _) <- accept s
    hSetBuffering h LineBuffering
    _ <- forkIO $ clientSession db transSet logger (TransactionID {clientName = hostName, transactionNum = initVal}) h hostName
    processRequests db transSet logger (initVal + 1000000) s

-- entry point, assuming initialization of the database.
runServer :: TVar Database -> TVar ActiveTransactions -> Log ->
             PortNumber -> IO ()
runServer db transSet logger port = do
    -- listen for connections and spin each one off into its own thread.
    bracket (listenOn (PortNumber port)) sClose (processRequests db transSet logger 0)
