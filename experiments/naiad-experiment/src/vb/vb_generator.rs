/*
    Timely code for the generator (data producer)
    in the Value Barrier example.
*/

extern crate timely;
#[macro_use] extern crate abomonation_derive;

use timely::dataflow::channels::pushers::tee::Tee;
use timely::dataflow::operators::{Inspect,Capability};
use timely::dataflow::operators::generic::{OperatorInfo,OutputHandle};
use timely::dataflow::operators::generic::operator::source;
use timely::dataflow::scopes::Scope;
use timely::dataflow::stream::Stream;

use std::time::{Duration, SystemTime};

mod vb_data;
use vb_data::{VBData, VBItem};

/*
    Utility functions related to time
*/

fn time_since(t: SystemTime) -> Duration {
    // Note: this function may panic in case of clock drift
    return t.elapsed().unwrap();
}
fn div_durations(d1: Duration, d2: Duration) -> u64 {
    ((d1.as_nanos() as f64) / (d2.as_nanos() as f64)) as u64
}

/*
    The main value source
*/

type Item = VBItem<u128>;

fn value_source<G>(scope: &G, loc: usize) -> Stream<G, Item>
where
    G: Scope<Timestamp = u64>,
{
    source(scope, "Source", |capability: Capability<u64>, info: OperatorInfo| {

        // Internal details of timely dataflow
        // 1. Acquire a re-activator for this operator.
        // 2. Wrap capability in an option so that we can release it when
        //    done by setting it to None
        let activator = scope.activator_for(&info.address[..]);
        let mut maybe_cap = Some(capability);

        // Parameters
        let frequency = Duration::from_micros(10);
        let total = Duration::from_secs(10);
        let vals_max = div_durations(total, frequency);

        // Initialize with current time
        let start_time = SystemTime::now();
        let mut vals_sent = 0;

        // Return closure
        move |output: &mut OutputHandle<u64, Item, Tee<u64, Item>>| {
            if let Some(cap) = maybe_cap.as_mut() {

                // Decide how behind we are on outputting values
                let elapsed = time_since(start_time);
                let vals_to_send = div_durations(elapsed, frequency);

                // Output values to catch up
                while vals_sent < vals_to_send &&
                      vals_sent < vals_max {
                    let elapsed = time_since(start_time);
                    let item = VBItem {
                        data: VBData::Value,
                        time: elapsed.as_nanos(),
                        loc: loc,
                    };
                    output.session(&cap).give(item);
                    vals_sent += 1;
                    cap.downgrade(&vals_sent);
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

/*
    Main function so that the generator can be run as a standalone
    binary without computation if desired
*/

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        let w_index = worker.index();
        worker.dataflow(|scope| {
            value_source(scope, w_index)
            .inspect(|x| println!("item generated: {:?}", x));
        });
    }).unwrap();
}
