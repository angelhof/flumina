/*
    Useful generator patterns for source data
*/

use super::util::{div_durations, time_since, nanos_timestamp};

use timely::dataflow::channels::pushers::tee::Tee;
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::generic::{OperatorInfo, OutputHandle};
use timely::dataflow::operators::generic::operator::source;
use timely::dataflow::scopes::Scope;
use timely::dataflow::stream::Stream;

use std::time::{Duration, SystemTime};

pub fn fixed_rate_source<D, F, G>(
    item_gen: F,
    scope: &G,
    frequency: Duration,
    total: Duration,
) -> Stream<G, D>
where
    D: timely::Data + timely::ExchangeData,
    F: Fn(u128) -> D + 'static,
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
        move |output: &mut OutputHandle<u128, D, Tee<u128, D>>| {
            if let Some(cap) = maybe_cap.as_mut() {

                // Decide how behind we are on outputting values
                let elapsed = time_since(start_time);
                let vals_to_send = div_durations(elapsed, frequency);

                // Output values to catch up
                while vals_sent < vals_to_send &&
                      vals_sent < vals_max {
                    let time_nanos = nanos_timestamp(SystemTime::now());
                    let item = item_gen(time_nanos);
                    cap.downgrade(&time_nanos);
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
