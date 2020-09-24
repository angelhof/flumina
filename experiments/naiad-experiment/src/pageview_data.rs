/*
    Timely code for the data in the Pageview example.
*/

use abomonation_derive::Abomonation;

use std::collections::HashMap;

pub type PVTimestamp = u128;
pub type PageName = u64;
pub type PageData = u64;

#[derive(Debug, PartialEq, Copy, Clone, Abomonation)]
pub enum PVData {
    View, // retrieve page metadata (an int)
    Update(PageData), // set page metadata (an int)
}

#[derive(Debug, PartialEq, Copy, Clone, Abomonation)]
pub struct PVItem {
    pub data: PVData,
    pub name: PageName, // page name
    // pub time: PVTimestamp, // timestamp
    // pub loc: usize, // node number
}

// #[derive(Debug, PartialEq, Copy, Clone)]
pub type PVState = HashMap<PageName, PageData>;
