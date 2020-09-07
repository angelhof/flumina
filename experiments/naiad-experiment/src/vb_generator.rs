/*
    Timely code for the generator (data producer)
    in the Value Barrier example.
*/

use super::util::{div_durations,time_since};
use super::vb_data::{VBData, VBItem};

use timely::dataflow::channels::pushers::tee::Tee;
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::generic::{OperatorInfo,OutputHandle};
use timely::dataflow::operators::generic::operator::source;
use timely::dataflow::scopes::Scope;
use timely::dataflow::stream::Stream;

use std::time::{Duration, SystemTime};

type Item = VBItem<u128>;

pub fn fixed_rate_source<G>(
    v_or_b: VBData,
    scope: &G,
    loc: usize,
    frequency: Duration,
    total: Duration,
) -> Stream<G, Item>
where
    G: Scope<Timestamp = u128>,
{
    source(scope, "Source", |capability: Capability<u128>, info: OperatorInfo| {

        // Internal details of timely dataflow
        // 1. Acquire a re-activator for this operator.
        // 2. Wrap capability in an option so that we can release it when
        //    done by setting it to None
        let activator = scope.activator_for(&info.address[..]);
        let mut maybe_cap = Some(capability);

        // Initialize with current time; keep track of # vals sent
        let start_time = SystemTime::now();
        let mut vals_sent = 0;
        let vals_max = div_durations(total, frequency);

        // Return closure
        move |output: &mut OutputHandle<u128, Item, Tee<u128, Item>>| {
            if let Some(cap) = maybe_cap.as_mut() {

                // Decide how behind we are on outputting values
                let elapsed = time_since(start_time);
                let vals_to_send = div_durations(elapsed, frequency);

                // Output values to catch up
                while vals_sent < vals_to_send &&
                      vals_sent < vals_max {
                    let elapsed_nanos = time_since(start_time).as_nanos();
                    let item = VBItem {
                        data: v_or_b,
                        time: elapsed_nanos,
                        loc: loc,
                    };
                    cap.downgrade(&elapsed_nanos);
                    output.session(&cap).give(item);
                    vals_sent += 1;
                    // cap.downgrade(&vals_sent);
                }
                if vals_sent == vals_max {
                    maybe_cap = None;
                }
                else {
                    activator.activate();
                }
            }
        }

    })
}

pub fn value_source<G>(
    scope: &G,
    loc: usize,
    frequency: Duration,
    total: Duration,
) -> Stream<G, Item>
where
    G: Scope<Timestamp = u128>,
{
    fixed_rate_source(VBData::Value, scope, loc, frequency, total)
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
    fixed_rate_source(VBData::Barrier, scope, loc, frequency, total)
}
