/*
    Useful generator patterns for source data
*/

use super::common::{Duration, Scope, Stream, SystemTime};
use super::util::{div_durations, nanos_timestamp, time_since};

use timely::dataflow::operators::generic::operator::source;

use std::cmp;

/*
    Data source which produces a number of output items
    over time given by an arbitrary function giving the cumulative
    total to produce, and stopping after the uptime is complete.

    For performance reasons, caps outputs at a given point in time
    by MAX_OUTPUT. See this note from
    https://github.com/TimelyDataflow/timely-dataflow
    > At the moment, the implementations of unary and binary operators
    > allow their closures to send un-bounded amounts of output. This
    > can cause unwelcome resource exhaustion, and poor performance
    > generally if the runtime needs to allocate lots of new memory to
    > buffer data sent in bulk without being given a chance to digest
    > it. It is commonly the case that when large amounts of data are
    > produced, they are eventually reduced given the opportunity.
*/
const MAX_OUTPUT: u128 = 1000;
fn variable_rate_source<D, F, G, H>(
    mut item_gen: F,
    scope: &G,
    cumulative_total_fun: H,
    uptime: Duration,
) -> Stream<G, D>
where
    D: timely::Data + timely::ExchangeData,
    F: FnMut(u128) -> D + 'static, // Input: timestamp in nanoseconds
    G: Scope<Timestamp = u128>,
    H: Fn(Duration) -> u128 + 'static, // Input: time sicne source start time
{
    source(scope, "Source", |capability, info| {
        // Internal details of timely dataflow
        // 1. Acquire a re-activator for this operator.
        // 2. Wrap capability in an option so that we can release it when
        //    done by setting it to None
        let activator = scope.activator_for(&info.address[..]);
        let mut maybe_cap = Some(capability);

        // Initialize with current time; keep track of # vals sent
        let start_time = SystemTime::now();
        let mut vals_sent = 0;
        let vals_max = cumulative_total_fun(uptime);

        // Return closure
        move |output| {
            if let Some(cap) = maybe_cap.as_mut() {
                // Decide how behind we are on outputting values
                let elapsed = time_since(start_time);
                let vals_to_send = cmp::min(
                    cumulative_total_fun(elapsed),
                    vals_sent + MAX_OUTPUT,
                );

                // For debugging (this is nice because it shows if the system is
                // under full load -- in this case # of new values will usually
                // be throttled at MAX_OUTPUT)
                // println!("New items to send: {}", vals_to_send - vals_sent);

                // Output values to catch up
                let time_nanos = nanos_timestamp(SystemTime::now());
                cap.downgrade(&time_nanos);
                while vals_sent < vals_to_send && vals_sent < vals_max {
                    let item = item_gen(time_nanos);
                    output.session(&cap).give(item);
                    vals_sent += 1;
                }
                if vals_sent == vals_max {
                    maybe_cap = None;
                } else {
                    activator.activate();
                }
            }
        }
    })
}

/*
    Data source which produces output items at a constant rate,
    stopping after the uptime is complete
*/
pub fn fixed_rate_source<D, F, G>(
    item_gen: F,
    scope: &G,
    frequency: Duration,
    uptime: Duration,
) -> Stream<G, D>
where
    D: timely::Data + timely::ExchangeData,
    F: FnMut(u128) -> D + 'static,
    G: Scope<Timestamp = u128>,
{
    variable_rate_source(
        item_gen,
        scope,
        move |elapsed| div_durations(elapsed, frequency),
        uptime,
    )
}

/*
    Data source which produces output items at a linearly increasing rate,
    stopping after the uptime is complete
*/
pub fn linear_rate_source<D, F, G>(
    item_gen: F,
    scope: &G,
    frequency_init: Duration,
    acceleration: f64, // output events / microsecond^2
    uptime: Duration,
) -> Stream<G, D>
where
    D: timely::Data + timely::ExchangeData,
    F: FnMut(u128) -> D + 'static,
    G: Scope<Timestamp = u128>,
{
    variable_rate_source(
        item_gen,
        scope,
        move |elapsed| {
            // Integral from 0 to T of (1 / f + a * t)
            //     = T / f + (a / 2) * T^2
            let micros_elapsed = elapsed.as_micros() as f64;
            div_durations(elapsed, frequency_init)
                + (((acceleration / 2.0) * micros_elapsed * micros_elapsed)
                    as u128)
        },
        uptime,
    )
}
