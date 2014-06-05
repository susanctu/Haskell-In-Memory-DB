{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ExistentialQuantification #-} 
{-# LANGUAGE RankNTypes #-}

{- Exporting all types, constructors, and accessors -}
module DBTypes (Tablename(..), ErrString(..), Database(..), Fieldname(..), Table(..), Column(..), Element(..), TransactionID(..), RowHash(..), LogOperation(..), Row(..), Log(..), ActiveTransactions(..)) where

import Data.Typeable 
import Control.Concurrent.STM
import Data.Map.Lazy
import Control.Concurrent.Chan
import Data.Set as S 
import Test.QuickCheck

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

{-How do this correctly?-}
{-instance Eq Element where
  (Element x)==(Element y)= case x of 
                              Just a -> case y of 
                                          Just b -> if typeOf a == typeOf b
                                                      then (a == b)
                                                      else False
                                          Nothing -> False
                              Nothing -> case y of 
                                           Just _ -> False
                                           Nothing -> True
                    

instance Ord Element where 
 -}

data TransactionID = TransactionID { clientName :: String 
                                   , transactionNum :: Int 
                                   } deriving(Eq, Ord, Show, Read)-- clientname, transaction number
 
data Row = Row {getField :: Fieldname -> STM(Maybe Element)}

newtype RowHash = RowHash Int deriving(Show, Read, Eq, Ord) 
data LogOperation = Start TransactionID
                  | TransactionLog TransactionID (Tablename, Fieldname, RowHash) (Maybe Element) (Maybe Element) -- last two are old val, new val
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
