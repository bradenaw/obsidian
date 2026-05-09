pub enum IteratorEither<A, B> {
    Left(A),
    Right(B),
}

impl<T, A: Iterator<Item = T>, B: Iterator<Item = T>> Iterator for IteratorEither<A, B> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            IteratorEither::Left(inner) => inner.next(),
            IteratorEither::Right(inner) => inner.next(),
        }
    }
}
