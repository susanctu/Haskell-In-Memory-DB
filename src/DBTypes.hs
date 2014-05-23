{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ExistentialQuantification #-} 
{-# LANGUAGE RankNTypes #-}

module DBTypes (Tablename, ErrString, LogString, Database, Fieldname, Table, Column) where

import Control.Concurrent.STM
import Data.Map.Lazy
 
newtype Tablename = Tablename String deriving(Show)
newtype ErrString = ErrString String deriving(Show)

data Database = Database { database :: Map Tablename (TVar Table) }
data Fieldname a = Fieldname { fieldname :: String }
data Table = forall a. (Show a, Ord a) => Table { primaryKey :: Maybe (Fieldname a) 
                                                , table :: Map (Fieldname a) (Column a)}
data Column a = Column { default_val :: Maybe a
                       , column :: TVar(Map RowHash (Element a))
                       } -- first element is default value
data Element a = Element { elem :: TVar a }

data TransactionID = TransactionID { clientName :: String 
                                   , transactionNum :: Int 
                                   } deriving(Show, Read)-- clientname, transaction number
 
newtype RowHash = RowHash Int deriving(Show, Read) 
data LogOperation = Start TransactionID
                  | forall a. (Show a, Ord a) => TransactionLog TransactionID (Tablename, Fieldname a, Rowhash) a a -- last two are old val, new val
                  | Commit TransactionID  
                  | StartCheckpoint [TransactionID]   
                  | EndCheckpoint 
                  | DropTable Tablename 
                  deriving (Show, Read) 


