/*
    Operators ("meters") for performance measurement of timely dataflow programs.

    These are not streaming operators; they compute their summaries after input
    is completely finished. They are designed this way for easier use in experiments.
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

/*
    Meter which computes latency statistics for an output stream.
*/
pub fn latency_meter<G, D>(
    stream: &Stream<G, D>,
    output_filename: &'static str,
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
                println!("Latencies: {:?}", latencies);
                vec_to_file(latencies.clone(), output_filename)
            }
        }
    });
}

/*
    Meter which computes the total volume on an input stream.
*/
pub fn volume_meter<G, D>(
    _in_stream: &Stream<G, D>,
) -> ()
where
    D: timely::Data,
    G: Scope<Timestamp = u128>,
{
    // TODO
    ()
}

/*
    Meter which computes the max timestamp on an output stream.
*/
pub fn completion_meter<G, D>(
    _out_stream: &Stream<G, D>,
) -> ()
where
    D: timely::Data,
    G: Scope<Timestamp = u128>,
{
    // TODO
    ()
}

/*
    Meter which computes the throughput on a computation from an input
    stream to an output stream.
*/
pub fn throughput_meter<G1, D1, G2, D2>(
    in_stream: &Stream<G1, D1>,
    out_stream: &Stream<G2, D2>,
    _output_filename: &str,
) -> ()
where
    D1: timely::Data,
    G1: Scope<Timestamp = u128>,
    D2: timely::Data,
    G2: Scope<Timestamp = u128>,
{
    volume_meter(in_stream);
    completion_meter(out_stream);
    // in_stream.binary_frontier(&out_stream, Pipeline, Pipeline)
}
