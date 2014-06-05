{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ExistentialQuantification #-} 
{-# LANGUAGE RankNTypes #-}

{- Exporting all types, constructors, and accessors -}
module DBTypes (Tablename(..), ErrString(..), Database(..), Fieldname(..), Table(..), Column(..), Element(..), TransactionID(..), RowHash(..), LogOperation(..), Row(..),
  Log(..), ActiveTransactions(..), construct_row, verify_row, transform_row) where

import Data.Typeable 
import Control.Concurrent.STM
import Control.Concurrent.Chan
import Test.QuickCheck
import Control.Monad
import Data.Map.Lazy (Map)
import Data.Set (Set)


newtype Tablename = Tablename String deriving(Ord, Eq, Show, Read)
newtype ErrString = ErrString String deriving(Show)

data Database = Database { database :: Map Tablename (TVar Table) }
data Fieldname = Fieldname String deriving(Ord, Eq, Show, Read)

instance Arbitrary Fieldname where
  arbitrary = elements [Fieldname "gdgdfs"]

instance Arbitrary Tablename where
  arbitrary = elements [Tablename "gdgdfs"]

data Table = Table { rowCounter :: Int 
                   , primaryKey :: Maybe Fieldname 
                   , table :: Map Fieldname Column}

data Column = Column { default_val :: Maybe Element
                     , col_type :: TypeRep
                     , column :: TVar(Map RowHash (TVar Element))
                     } -- first element is default value

data Element = forall a. (Show a, Ord a, Eq a, Read a, Typeable a) => Element (Maybe a) -- Nothing here means that it's null

instance Show Element where
  show (Element x) = show x

instance Read Element where
  readsPrec _ str = [(fst (head (readsPrec 0 str)),"")]

instance Eq Element where
  (Element mx) == (Element my) = case cast mx of Just typed_mx -> typed_mx == my
                                                 Nothing       -> False

data TransactionID = TransactionID { clientName :: String 
                                   , transactionNum :: Int 
                                   } deriving(Eq, Ord, Show, Read)-- clientname, transaction number
 
data Row = Row {getField :: Fieldname -> STM(Maybe Element)}

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

newtype RowHash = RowHash Int deriving(Show, Read, Eq, Ord) 
data LogOperation = Start TransactionID
                  | Insert TransactionID (Tablename, RowHash) [(Fieldname, Element)]
                  | Delete TransactionID (Tablename, RowHash) [(Fieldname, Element)]
                  | Update TransactionID (Tablename, RowHash) [(Fieldname, Element, Element)] -- last two are old val, new val
                  | Commit TransactionID  
                  | StartCheckpoint [TransactionID]   
                  | EndCheckpoint
                  | DropTable TransactionID Tablename 
                  | CreateTable TransactionID Tablename
                  | AddField TransactionID Tablename Fieldname
                  | DropField TransactionID Tablename Fieldname
                  | SetPrimaryKey TransactionID (Maybe Fieldname) (Maybe Fieldname) Tablename -- old field, new field
                  deriving (Show, Read) 

type Log = Chan LogOperation
type ActiveTransactions = Set TransactionID
