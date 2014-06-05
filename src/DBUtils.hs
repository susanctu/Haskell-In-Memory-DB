module DBUtils (construct_row, verify_row, transform_row, output_table, read_table)
where

import Control.Concurrent.STM
import Control.Monad
import DBTypes
import Data.Typeable
import Data.Maybe
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

from_table :: Table -> STM ShowableTable
from_table table = return $ ShowableTable 0 Nothing Map.empty

from_column :: Column -> STM ShowableColumn
from_column col = return $ ShowableColumn Nothing "" Map.empty

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
