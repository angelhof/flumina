/*
    Timely code for the generator (data producer)
    in the Value Barrier example.
*/

use timely::dataflow::channels::pushers::tee::Tee;
use timely::dataflow::operators::{Inspect,Capability};
use timely::dataflow::operators::generic::{OperatorInfo,OutputHandle};
use timely::dataflow::operators::generic::operator::source;
use timely::dataflow::scopes::Scope;
use timely::dataflow::stream::Stream;

use std::io;
use std::fmt::Debug;
use std::str::FromStr;
use std::time::{Duration, SystemTime};

mod vb_data;
use vb_data::{VBData, VBItem};

/*
    Utility functions
*/

// Related to time
fn time_since(t: SystemTime) -> Duration {
    // Note: this function may panic in case of clock drift
    return t.elapsed().unwrap();
}
fn div_durations(d1: Duration, d2: Duration) -> u64 {
    ((d1.as_nanos() as f64) / (d2.as_nanos() as f64)) as u64
}

// For stdin input
fn get_input<T>(msg: &str) -> T
where
    T: FromStr,
    <T as FromStr>::Err: Debug,
{
    println!("{}", msg);
    let mut input_text = String::new();
    io::stdin()
        .read_line(&mut input_text)
        .expect("failed to read from stdin");
    input_text.trim().parse::<T>().expect("not an integer")
}

/*
    The main value source
*/

type Item = VBItem<u128>;

fn value_source<G>(
    scope: &G,
    loc: usize,
    frequency: Duration,
    total: Duration,
) -> Stream<G, Item>
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

        // Initialize with current time; keep track of # vals sent
        let start_time = SystemTime::now();
        let mut vals_sent = 0;
        let vals_max = div_durations(total, frequency);

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

    let frequency = Duration::from_micros(get_input("Frequency in microseconds:"));
    let total = Duration::from_secs(get_input("Total time to run in seconds:"));

    timely::execute_from_args(std::env::args(), move |worker| {
        let w_index = worker.index();
        worker.dataflow(|scope| {
            value_source(scope, w_index, frequency, total)
            .inspect(|x| println!("item generated: {:?}", x));
        });
    }).unwrap();
}
