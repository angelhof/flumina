/*
Timely code for the data used in the Value Barrier example.
*/

#[derive(Debug, PartialEq, Copy, Clone, Abomonation)]
pub enum VBData {
    Value, // alternative: Value(u64)
    Barrier,
}

#[derive(Debug, PartialEq, Copy, Clone, Abomonation)]
pub struct VBItem<T> {
    pub data: VBData,
    pub time: T,  // timestamp
    pub loc: usize, // node number
}
