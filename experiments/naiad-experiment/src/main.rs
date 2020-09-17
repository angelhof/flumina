/*
    Command-line entrypoint to run experiments.
*/

use naiad_experiment::vb::{vb_experiment_main,vb_experiment_gen_only};

use clap::{Arg, App, AppSettings, SubCommand};

use std::time::Duration;

fn main() {
    let command_vb = SubCommand::with_name("vb")
        .about("Value-Barrier experiment")
        .arg(Arg::with_name("VAL_FREQ")
            .help("frequency of values in microseconds")
            .index(1)
            .required(true))
            // .validator(|s| { s.parse::<u64>(); }))
        .arg(Arg::with_name("BAR_FREQ")
            .help("frequency of barriers in microseconds")
            .index(2)
            .required(true))
            // .validator(|s| { s.parse::<u64>(); }))
        .arg(Arg::with_name("DURATION")
            .help("experiment duration in seconds")
            .index(3)
            .required(true))
        .arg(Arg::with_name("gen-only")
            .short("g")
            .help("data generation only (no processing)"
        ));
            // .validator(|s| { s.parse::<u64>(); }));

    let app = App::new("naiad-experiment")
        .author("Caleb Stanford")
        .about("Command line for running Naiad (Timely Dataflow) experiments")
        .setting(AppSettings::SubcommandRequired)
        .subcommand(command_vb);
    
    let matches = app.clone().get_matches();

    if let Some(matches) = matches.subcommand_matches("vb") {
        // Value Barrier Command
        // The following parsing may panic
        let val_frequency = Duration::from_micros(
            matches.value_of("VAL_FREQ").unwrap()
            .parse::<u64>().unwrap()
        );
        let bar_frequency = Duration::from_micros(
            matches.value_of("BAR_FREQ").unwrap()
            .parse::<u64>().unwrap()
        );
        let exp_duration = Duration::from_micros(
            matches.value_of("DURATION").unwrap()
            .parse::<u64>().unwrap()
        );

        if matches.is_present("-g") {
            vb_experiment_gen_only(val_frequency, bar_frequency, exp_duration);
        }
        else {
            vb_experiment_main(val_frequency, bar_frequency, exp_duration);
        }
    }

}
