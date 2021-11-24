/*
    Timely code for the generators (data producers)
    specific to the Value Barrier example.
*/

use super::common::{Duration, Scope, Stream};
use super::generators::fixed_rate_source;
use super::util::rand_range;
use super::vb_data::{VBData, VBItem};

pub fn value_source<G>(
    scope: &G,
    loc: usize,
    frequency: Duration,
    total: Duration,
) -> Stream<G, VBItem>
where
    G: Scope<Timestamp = u128>,
{
    let item_gen = move |time| {
        let value = rand_range(0, 1000) as usize;
        VBItem { data: VBData::Value(value), time, loc }
    };
    fixed_rate_source(item_gen, scope, frequency, total)
}

pub fn barrier_source<G>(
    scope: &G,
    loc: usize,
    heartbeat_frequency: Duration,
    heartbeats_per_barrier: u64,
    total: Duration,
) -> Stream<G, VBItem>
where
    G: Scope<Timestamp = u128>,
{
    let mut count = 0;
    let item_gen = move |time| {
        count += 1;
        if count % heartbeats_per_barrier == 0 {
            VBItem { data: VBData::Barrier, time, loc }
        } else {
            VBItem { data: VBData::BarrierHeartbeat, time, loc }
        }
    };
    fixed_rate_source(item_gen, scope, heartbeat_frequency, total)
}
