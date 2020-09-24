/*
    Timely code for the data used in the Value Barrier example.
*/

use abomonation_derive::Abomonation;

type VBTimestamp = u128;

#[derive(Debug, PartialEq, Copy, Clone, Abomonation)]
pub enum VBData {
    Value, // alternative: Value(u64)
    Barrier,
    BarrierHeartbeat,
}

#[derive(Debug, PartialEq, Copy, Clone, Abomonation)]
pub struct VBItem {
    pub data: VBData,
    pub time: VBTimestamp,
    pub loc: usize, // node number
}
