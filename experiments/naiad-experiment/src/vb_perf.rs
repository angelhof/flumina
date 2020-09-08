/*
    Timely code for the performance measurement
    in the Value Barrier example.
*/

use super::util::{nanos_timestamp,vec_to_file};

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::channels::pushers::tee::Tee;
use timely::dataflow::operators::{Capability,Operator};
use timely::dataflow::operators::generic::{OperatorInfo,OutputHandle};
use timely::dataflow::scopes::Scope;
use timely::dataflow::stream::Stream;

use std::time::SystemTime;
use std::vec::Vec;

pub fn latency_meter<G, D>(
    stream: &Stream<G, D>,
) -> ()
where
    D: timely::Data,
    G: Scope<Timestamp = u128>,
{
    stream.unary_frontier(Pipeline, "Latency Meter",
                          |_capability: Capability<u128>, _info: OperatorInfo| {

        let mut count = 0;
        // let start_time = SystemTime::now();
        let mut latencies = Vec::new();

        let mut input_list = Vec::new();

        move |input, _output: &mut OutputHandle<u128, D, Tee<u128, D>>| {
            while let Some((capability, data)) = input.next() {
                let time = capability.time();
                data.swap(&mut input_list);
                // let mut session = output.session(&time);
                for _input in input_list.drain(..) {
                    // Core performance measuring logic
                    let timestamp_now = nanos_timestamp(SystemTime::now());
                    let latency = timestamp_now - time;
                    latencies.push(latency);
                    // println!("latency: {:?}", latency);
                    count += 1;
                }
            }
            // input.next() is None
            // Check if entire input is done
            if input.frontier().is_empty() && latencies.len() > 0 {
                // println!("Latencies: {:?}", latencies);
                vec_to_file(latencies.clone(), "latencies.out")
            }
        }
    });
}

pub fn throughput_meter() -> () {
}
