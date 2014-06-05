module DBUtils (construct_row, verify_row, transform_row,
                output_table, read_table,
                isCharType, isBitType, readType)
where

import Control.Concurrent.STM
import Control.Monad

import DBTypes
import Data.Typeable
import Data.Int
import qualified Data.ByteString as B
import qualified Data.Map.Lazy as Map

data ShowableTable = ShowableTable { rowCounter :: Int
                                   , primaryKey :: Maybe Fieldname
                                   , table :: Map.Map Fieldname ShowableColumn} deriving (Show, Read)

data ShowableColumn = ShowableColumn { default_val :: Maybe Element
                                     , col_type :: String
                                     , column :: Map.Map RowHash Element} deriving (Show,Read)

output_table :: Table -> IO String
output_table table = atomically $ do showable <- from_table table
                                     return $ show showable

--colToTable :: TVar(Map RowHash (TVar Element) -> STM (Map.Map RowHash Element)

from_table :: Table -> STM ShowableTable
from_table tab = do
    let stage1 = map (\(f, c) -> (f, from_column c)) $ Map.toList $ DBTypes.table tab
        stage2 = map (\(f, stc) -> do
            col <- stc -- should propagate the STM type
            return (f, col)) stage1
    colMap <- Map.fromList $ sequence stage2
    return $ ShowableTable (DBTypes.rowCounter tab) (DBTypes.primaryKey tab) colMap

from_column :: Column -> STM ShowableColumn
from_column = undefined
{-from_column col = do
    colData' <- readTVar $ column col
    colData <- 
    return $ ShowableColumn (default_val col) (col_type col) colData
-}
read_table :: String -> IO (TVar Table)
read_table str = atomically $ to_table $ read str

to_table :: ShowableTable -> STM (TVar Table)
to_table show_table = newTVar $ Table 0 Nothing Map.empty

to_column :: ShowableColumn -> STM (TVar Column)
to_column show_col = do col <- newTVar Map.empty
                        newTVar $ Column Nothing (typeOf "JI") col

construct_row :: [(Fieldname, Element)] -> Row
construct_row content = Row $ return . (flip lookup content)

verify_row :: [(Fieldname, Element->Bool)] -> Row -> STM Bool
verify_row content row = foldr (liftM2 (&&)) (return True) $ zipWith liftM (map (evaluate . snd) content) $ map (getField row) (map fst content) 
    where evaluate f Nothing   = False
          evaluate f (Just x)  = f x

transform_row :: [(Fieldname, Element -> Element)] -> Row -> Row
transform_row content row = let transformed = zip (map fst content) $ zipWith liftM (map (evaluate . snd) content) $ map (getField row) (map fst content) --type is [(Fieldname, STM Maybe Element)]
                              in Row $ \f -> case lookup f transformed of Just x  -> x
                                                                          Nothing -> return Nothing
    where evaluate f Nothing  = Nothing
          evaluate f (Just x) = Just (f x)

-- char(n) or varchar(n)
isCharType :: String -> Bool
isCharType s = take 4 s == "char" || take 7 s == "varchar"

-- Haskell doesn't seem to have a native bit-array type, so not clear what we should do.
-- bit(n) or bitvarying(n)
isBitType :: String -> Bool
isBitType s = take 3 s == "bit" -- since there are only so many types

readType :: String -> TypeRep
readType ftype
    | ftype == "boolean" = typeOf(undefined :: Bool)
    | isCharType ftype || isBitType ftype = typeOf(undefined :: B.ByteString)
    | ftype == "integer" = typeOf(undefined :: Int32)
    | ftype == "real"    = typeOf(undefined :: Double)
    | otherwise = typeOf(undefined :: B.ByteString)
