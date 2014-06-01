{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ExistentialQuantification #-} 
{-# LANGUAGE RankNTypes #-}

{- Exporting all types, constructors, and accessors -}
module DBTypes (Tablename(..), ErrString(..), Database(..), Fieldname(..), Table(..), Column(..), Element(..), TransactionID(..), RowHash(..), LogOperation(..)) where

import Control.Concurrent.STM
import Data.Map.Lazy
 
newtype Tablename = Tablename String deriving(Eq, Show)
newtype ErrString = ErrString String deriving(Show)

data Database = Database { database :: Map Tablename (TVar Table) }
data Fieldname a = Fieldname { fieldname :: String } deriving(Eq, Show)

data Table = forall a. (Show a, Ord a) => Table { rowCounter :: Int 
                                                , primaryKey :: Maybe (Fieldname a) 
                                                , table :: Map (Fieldname a) (Column a)}
data Column a = Column { default_val :: Maybe (Element a)
                       , column :: TVar(Map RowHash (Element a))
                       } -- first element is default value

data Element a = Element { element :: TVar (Maybe a) }

data TransactionID = TransactionID { clientName :: String 
                                   , transactionNum :: Int 
                                   } deriving(Eq, Show, Read)-- clientname, transaction number
 
 data Row = Row {getField :: Fieldname a -> a}
newtype RowHash = RowHash Int deriving(Show, Read) 
data LogOperation = Start TransactionID
                  | forall a. (Show a, Ord a) => TransactionLog TransactionID (Tablename, Fieldname a, Rowhash) (Maybe a) (Maybe a) -- last two are old val, new val
                  | Commit TransactionID  
                  | StartCheckpoint [TransactionID]   
                  | EndCheckpoint 
                  | DropTable TransactionID Tablename 
                  | CreateTable TransactionID Tablename
                  | AddField TransactionID Tablename Fieldname
                  | DropField TransactionID Tablename Fieldname
                  | SetPrimaryKey TransactionID (Maybe Fieldname) (Maybe Fieldname) Tablename -- old field, new field
                  deriving (Show, Read) 


