/*
    Timely code for the generators (data producers)
    specific to the Value Barrier example.
*/

use super::common::{Duration, Scope, Stream};
use super::generators::fixed_rate_source;
use super::vb_data::{VBData, VBItem};

type Item = VBItem<u128>;

pub fn value_source<G>(
    scope: &G,
    loc: usize,
    frequency: Duration,
    total: Duration,
) -> Stream<G, Item>
where
    G: Scope<Timestamp = u128>,
{
    let item_gen = move |time| {
        VBItem { data: VBData::Value, time: time, loc: loc }
    };
    fixed_rate_source(item_gen, scope, frequency, total)
}

pub fn barrier_source<G>(
    scope: &G,
    loc: usize,
    frequency: Duration,
    total: Duration,
) -> Stream<G, Item>
where
    G: Scope<Timestamp = u128>,
{
    let item_gen = move |time| {
        VBItem { data: VBData::Barrier, time: time, loc: loc }
    };
    fixed_rate_source(item_gen, scope, frequency, total)
}
