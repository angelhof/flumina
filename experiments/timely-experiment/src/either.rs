/*
    Simple enum with two variants.
*/

use abomonation_derive::Abomonation;

#[derive(Debug, PartialEq, Copy, Clone, Abomonation)]
pub enum Either<D1, D2> {
    Left(D1),
    Right(D2),
}
