{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ExistentialQuantification #-} 
{-# LANGUAGE RankNTypes #-}

{- Exporting all types, constructors, and accessors -}
module DBTypes (Tablename(..), ErrString(..), Database(..), Fieldname(..), Table(..), Column(..), Element(..), TransactionID(..), RowHash(..), LogOperation(..), Row(..), Log(..)) where

import Data.Typeable 
import Control.Concurrent.STM
import Data.Map.Lazy
import Control.Concurrent.Chan
 
newtype Tablename = Tablename String deriving(Eq, Show, Read)
newtype ErrString = ErrString String deriving(Show)

data Database = Database { database :: Map Tablename (TVar Table) }
data Fieldname = Fieldname { fieldname :: String } deriving(Eq, Show, Read)

data Table = Table { rowCounter :: Int 
                   , primaryKey :: Maybe Fieldname 
                   , table :: Map Fieldname Column}

data Column = Column { default_val :: Maybe Element
                     , col_type :: TypeRep
                     , column :: TVar(Map RowHash (TVar Element))
                     } -- first element is default value

data Element = forall a. (Show a, Ord a, Read a) => Element (Maybe a) -- Nothing here means that it's null

instance Show Element where
  show (Element x) = show x

instance Read Element where
  readsPrec _ str = [(fst (head (readsPrec 0 str)),"")]

data TransactionID = TransactionID { clientName :: String 
                                   , transactionNum :: Int 
                                   } deriving(Eq, Show, Read)-- clientname, transaction number
 
data Row = Row {getField :: Fieldname -> Maybe Element}

newtype RowHash = RowHash Int deriving(Show, Read) 
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
