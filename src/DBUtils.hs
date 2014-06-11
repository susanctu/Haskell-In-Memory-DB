module DBUtils (construct_row, verify_row, transform_row,
                output_table, read_table,
                isCharType, isBitType, readType)
where

import Control.Concurrent.STM
import Control.Monad

import DBTypes
import Data.Typeable
import qualified Data.ByteString as B
import qualified Data.Map.Lazy as Map
import qualified Data.Traversable as T

data ShowableTable = ShowableTable { rowCounter :: Int
                                   , primaryKey :: Maybe Fieldname
                                   , table :: Map.Map Fieldname ShowableColumn} deriving (Show, Read)

data ShowableColumn = ShowableColumn { default_val :: Maybe Element
                                     , col_type :: String
                                     , column :: Map.Map RowHash Element} deriving (Show,Read)

output_table :: Table -> IO String
output_table t = atomically $ do showable <- from_table t
                                 return $ show showable

from_table :: Table -> STM ShowableTable
from_table (Table rc pk t) =  do t' <- T.mapM from_column t
                                 return $ ShowableTable rc pk t'

from_column :: Column -> STM ShowableColumn
from_column (Column dv ct col) = do extractedCol <- readTVar col
                                    col' <-  T.mapM readTVar extractedCol
                                    return $ ShowableColumn dv (show ct) col'

read_table :: String -> IO (TVar Table)
read_table str = atomically $ to_table $ read str

to_table :: ShowableTable -> STM (TVar Table)
to_table (ShowableTable rc pk t) = do  t' <- T.mapM to_column t
                                       newTVar $ Table rc pk t'

to_column :: ShowableColumn -> STM Column
to_column (ShowableColumn dv ct col) = do col1 <- T.mapM newTVar col
                                          col2 <- newTVar col1
                                          return $ Column dv (readType ct) col2

construct_row :: [(Fieldname, Element)] -> Row
construct_row content = Row $ return . (flip lookup content)

verify_row :: [(Fieldname, Element->Bool)] -> Row -> STM Bool
verify_row content row = foldr (liftM2 (&&)) (return True) $ zipWith liftM (map (evaluate . snd) content) $ map (getField row) (map fst content) 
    where evaluate _ Nothing   = False
          evaluate f (Just x)  = f x

transform_row :: [(Fieldname, Element -> Element)] -> Row -> Row
transform_row content row = let transformed = zip (map fst content) $ zipWith liftM (map (evaluate . snd) content) $ map (getField row) (map fst content) --type is [(Fieldname, STM Maybe Element)]
                              in Row $ \f -> case lookup f transformed of Just x  -> x
                                                                          Nothing -> return Nothing
    where evaluate _ Nothing  = Nothing
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
    | ftype == "integer" = typeOf(undefined :: Int)
    | ftype == "real"    = typeOf(undefined :: Double)
    | otherwise = typeOf(undefined :: B.ByteString)
