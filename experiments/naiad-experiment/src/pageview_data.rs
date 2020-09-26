/*
    Timely code for the data in the Pageview example.
*/

use abomonation_derive::Abomonation;

pub type PVTimestamp = u128;
pub type PageName = u64;
pub type PageData = u64;

#[derive(Debug, PartialEq, Copy, Clone, Abomonation)]
pub enum PVData {
    View,             // retrieve page metadata (an int)
    Update(PageData), // set page metadata (an int)
}

#[derive(Debug, PartialEq, Copy, Clone, Abomonation)]
pub struct PVItem {
    pub data: PVData,
    pub name: PageName, // page name
}
// Fields that could be added:
// pub time: PVTimestamp,
// pub loc: usize, // node number
