/*
Timely code for the data used in the Value Barrier example.
*/

#[derive(Debug, PartialEq, Copy, Clone, Abomonation)]
pub enum VBData {
    Value, // alternative: Value(u64)
    Barrier,
}

// unsafe_abomonate!(VBData);

#[derive(Debug, PartialEq, Copy, Clone, Abomonation)]
pub struct VBItem {
    pub data: VBData,
    pub time: i64,  // timestamp
    pub loc: usize, // node number
}

// #[allow(deprecated)]
// unsafe_abomonate!(VBItem : data, time, loc);
