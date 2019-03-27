module AWS.DynamoDB.Data
  ( class AttributeValue
  , class AttributeRecord
  , class AttributeValueRecord
  , encodeValue
  , decodeValue
  , encodeRecord
  , decodeRecord
  , encodeValueRecord
  , decodeValueRecord
  ) where

import Prelude

import Control.Monad.Except (mapExcept)
import Data.Array as Array
import Data.Bifunctor (lmap)
import Data.List (List)
import Data.Maybe (Maybe(..), maybe)
import Data.Set (Set)
import Data.Set as Set
import Data.Symbol (class IsSymbol, SProxy(..), reflectSymbol)
import Data.Traversable (traverse)
import Foreign (F, Foreign, ForeignError(..))
import Foreign.Generic (decodeJSON, defaultOptions)
import Foreign.Generic.Class (class Decode_, class Encode_, decode_, encode_)
import Foreign.Index (hasOwnProperty, index)
import Foreign.Object (Object)
import Foreign.Object as Object
import Prim.Row (class Cons, class Lacks)
import Prim.RowList (class RowToList, Nil, Cons)
import Record as Record
import Type.Data.RowList (RLProxy(..))
import Unsafe.Coerce (unsafeCoerce)


encode :: forall a. Encode_ a => a -> Foreign
encode = encode_ defaultOptions

decode :: forall a. Decode_ a => Foreign -> F a
decode = decode_ defaultOptions

class AttributeValue a where
  encodeValue :: a -> Foreign
  decodeValue :: Foreign -> F a

class AttributeRecord r where
  encodeRecord :: Record r -> Foreign
  decodeRecord :: Foreign -> F (Record r)

class AttributeValueRecord r rl | rl -> r where
  encodeValueRecord :: RLProxy rl -> Record r -> Object Foreign
  decodeValueRecord :: RLProxy rl -> Foreign -> F (Record r)

instance attributeRecord :: (RowToList r rl, AttributeValueRecord r rl) => AttributeRecord r where
  encodeRecord = unsafeCoerce <<< encodeValueRecord (RLProxy :: _ rl)
  decodeRecord = decodeValueRecord (RLProxy :: _ rl)

instance attributeValueForeign :: AttributeValue Foreign where
  encodeValue = identity
  decodeValue = pure

instance attributeValueString :: AttributeValue String where
  encodeValue x = encode { "S": x }
  decodeValue x = do
    { "S": s } <- decode x :: _ { "S" :: _ }
    pure s

instance attributeValueInt :: AttributeValue Int where
  encodeValue x = encode { "N": show x }
  decodeValue x = do
    { "N": n } <- decode x :: _ { "N" :: String }
    decodeJSON n

instance attributeValueNumber :: AttributeValue Number where
  encodeValue x = encode { "N": show x }
  decodeValue x = do
    { "N": n } <- decode x :: _ { "N" :: String }
    decodeJSON n

instance attributeValueSetString :: AttributeValue (Set String) where
  encodeValue x = encode { "SS": Set.toUnfoldable x :: Array String }
  decodeValue x = do
    { "SS": ss } <- decode x :: _ { "SS" :: Array String }
    pure $ Set.fromFoldable ss
else instance attributeValueSetInt :: AttributeValue (Set Int) where
  encodeValue x = encode { "NS": map show $ Set.toUnfoldable x :: Array Int }
  decodeValue x = do
    { "NS": ns } <- decode x :: _ { "NS" :: Array String }
    ints <- traverse decodeJSON ns
    pure $ Set.fromFoldable ints
else instance attributeValueSetNumber :: AttributeValue (Set Number) where
  encodeValue x = encode { "NS": map show $ Set.toUnfoldable x :: Array Number }
  decodeValue x = do
    { "NS": ns } <- decode x :: _ { "NS" :: Array String }
    numbers <- traverse decodeJSON ns
    pure $ Set.fromFoldable numbers
else instance attributeValueSet :: (Ord a, AttributeValue a) => AttributeValue (Set a) where
  encodeValue = encodeValue <<< (Set.toUnfoldable :: _ -> Array _)
  decodeValue = decodeValue >>> map (Set.fromFoldable :: Array _ -> _)

instance attributeValueObject :: AttributeValue a => AttributeValue (Object a) where
  encodeValue x = encode { "M": map encodeValue x }
  decodeValue x = do
    { "M": m } <- decode x :: _ { "M" :: Object Foreign }
    traverse decodeValue m

instance attributeValueRecord :: (AttributeRecord r) => AttributeValue (Record r) where
  encodeValue x = encode { "M": encodeRecord x }
  decodeValue x = do
    { "M": m } <- decode x :: _ { "M" :: Foreign }
    decodeRecord m

instance attributeValueArray :: AttributeValue a => AttributeValue (Array a) where
  encodeValue x = encode { "L": map encodeValue x }
  decodeValue x = do
    { "L": l } <- decode x :: _ { "L" :: Array Foreign }
    traverse decodeValue l

instance attributeValueList :: AttributeValue a => AttributeValue (List a) where
  encodeValue = encodeValue <<< Array.fromFoldable
  decodeValue = decodeValue >>> map Array.toUnfoldable

instance attributeValueBoolean :: AttributeValue Boolean where
  encodeValue x = encode { "BOOL": x }
  decodeValue x = do
    { "BOOL": b } <- decode x :: _ { "BOOL" :: Boolean }
    pure b

instance attributeValueRecordNil :: AttributeValueRecord () Nil where
  encodeValueRecord _ _ = Object.empty
  decodeValueRecord _ _ = pure {}

instance attributeValueRecordConsMaybe ::
  (Cons l (Maybe a) r_ r, AttributeValueRecord r_ rl_, IsSymbol l, AttributeValue a, Lacks l r_)
    => AttributeValueRecord r (Cons l (Maybe a) rl_ ) where
  encodeValueRecord _ rec =
    let val = Record.get (SProxy :: _ l) rec
        obj = encodeValueRecord (RLProxy :: RLProxy rl_) (unsafeCoerce rec)
        l = reflectSymbol (SProxy :: SProxy l)
    in maybe obj (\a -> Object.insert l (encodeValue a) obj) val
  decodeValueRecord _ f = do
    r <- decodeValueRecord (RLProxy :: RLProxy rl_) f
    let l = reflectSymbol (SProxy :: SProxy l)
    if hasOwnProperty l f
      then do
        f_ <- index f l
        a <- mapExcept (lmap (map (ErrorAtProperty l))) (decodeValue f_)
        pure (Record.insert (SProxy :: SProxy l) (Just a) r)
      else
        pure (Record.insert (SProxy :: SProxy l) Nothing r)
else instance attributeValueRecordCons ::
  (Cons l a r_ r, AttributeValueRecord r_ rl_, IsSymbol l, AttributeValue a, Lacks l r_)
    => AttributeValueRecord r (Cons l a rl_ ) where
  encodeValueRecord _ rec =
    let obj = encodeValueRecord (RLProxy :: RLProxy rl_) (unsafeCoerce rec)
        l = reflectSymbol (SProxy :: SProxy l)
    in Object.insert l (encodeValue (Record.get (SProxy :: SProxy l) rec)) obj
  decodeValueRecord _ f = do
    r <- decodeValueRecord (RLProxy :: RLProxy rl_) f
    let l = reflectSymbol (SProxy :: SProxy l)
    f_ <- index f l
    a <- mapExcept (lmap (map (ErrorAtProperty l))) (decodeValue f_)
    pure (Record.insert (SProxy :: SProxy l) a r)
