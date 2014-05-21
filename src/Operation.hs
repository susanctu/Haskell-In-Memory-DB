{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ExistentialQuantification #-} 
{-# LANGUAGE RankNTypes #-}
module Operation (Tablename, ErrString, LogString, Database, Fieldname, Table, Column, create_table, drop_table, alter_table_add, alter_table_drop, select, Operation.insert, Operation.update, Operation.delete, show_tables) where

import System.IO
import Control.Monad
import Control.Concurrent
import Control.Concurrent.STM
import Data.Map.Lazy
{- First stab at the interface -}
-- Note that this means that you'll have to keep track of the type 
-- for each table/col combo outside of this module since you need to 
-- parse the string into the correct haskell type before you attempt
-- inserts, for example. This means lookups on some Fieldname a should
-- never fail, but this module will handle the case when it does.

newtype Tablename = Tablename String deriving(Show)
newtype ErrString = ErrString String deriving(Show)
newtype LogString  = LogString String deriving(Show)

data Database = Database (Map Tablename (TVar Table))
data Fieldname a = Fieldname String a
data Table = forall a. (Show a, Ord a) => Table (Map (Fieldname a) (Column a))
data Column a = Column (Maybe a) (TVar([Element a])) -- first element is default value
data Element a = Element (TVar a)

create_table :: TVar Database -> Tablename -> (forall a. [(Fieldname a, Column a)]) -> Maybe (Fieldname b) -> STM (IO ErrString, IO LogString)
create_table db tablename field_and_col primary_key = return (return (ErrString ""), return (LogString ""))

drop_table :: TVar Database -> String -> STM (IO ErrString, IO LogString)
drop_table db tablename = return (return (ErrString ""), return (LogString ""))

alter_table_add :: TVar Database -> String -> Fieldname a -> STM (IO ErrString, IO LogString) 
alter_table_add db tablename fieldname = return (return (ErrString ""), return (LogString "")) 

alter_table_drop :: TVar Database -> String -> String -> STM (IO ErrString, IO LogString) 
alter_table_drop db tablename fieldname = return (return (ErrString ""), return (LogString ""))

select :: TVar Database -> (forall a. [(Fieldname a, (a -> Bool))]) -> Tablename -> STM(IO ErrString, IO LogString, IO String) -- last string is the stuff user queried for
select db fieldnames_and_conds tablename = return (return (ErrString ""), return (LogString ""), return "") 

insert :: TVar Database -> Tablename -> (forall a. [(Fieldname a, a)]) -> STM(IO ErrString, IO LogString) 
insert db tablename fieldnames_and_vals = return (return (ErrString ""), return (LogString ""))

delete :: TVar Database -> Tablename -> (forall a. [(Fieldname a, (a -> Bool))]) -> STM(IO ErrString, IO LogString)
delete db tablename filenames_and_conds = return (return (ErrString ""), return (LogString ""))   

update :: TVar Database -> Tablename -> (forall a. [(Fieldname a, a, (a -> Bool))]) -> STM (IO ErrString, IO LogString)
update db tablename filenames_vals_conds = return (return (ErrString ""), return (LogString ""))

show_tables :: TVar Database -> STM (IO String) -- doesn't actually update the db, so no need for logstring
show_tables db = return (return "") 


