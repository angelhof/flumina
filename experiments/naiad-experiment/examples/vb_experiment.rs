/*
    Input parameters, then run a value barrier experiment.
*/

use naiad_experiment::util;
use naiad_experiment::vb::vb_experiment_main;

use std::time::Duration;

fn main() {
    let value_frequency = Duration::from_micros(
        util::get_input("Value frequency in microseconds:")
    );
    let barrier_frequency = Duration::from_millis(
        util::get_input("Barrier frequency in milliseconds:")
    );
    let experiment_duration = Duration::from_secs(
        util::get_input("Total time to run in seconds:")
    );

    vb_experiment_main(value_frequency, barrier_frequency, experiment_duration);
}
