module Kinesis.Utils where


whenJust :: Monad m => Maybe t -> (t -> m ()) -> m ()
whenJust a f = case a of
  Nothing -> return ()
  Just a' -> f a'

