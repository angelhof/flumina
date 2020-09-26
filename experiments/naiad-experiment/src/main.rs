/*
    Command-line entrypoint to run experiments.
*/

use naiad_experiment::pageview::PVExperimentParams;
use naiad_experiment::util::{current_datetime_str, string_to_static_str};
use naiad_experiment::vb::VBExperimentParams;

use clap::{App, AppSettings, Arg, SubCommand};

/* Results filenames */

const RESULTS_DIR: &str = "results/";
const RESULTS_EXT: &str = ".out";

fn make_results_path(exp_name: &str, args: &[&str]) -> &'static str {
    let mut out = RESULTS_DIR.to_owned() + &current_datetime_str() + "_";
    out += exp_name;
    for arg in args {
        out += "_";
        out += arg;
    }
    out += RESULTS_EXT;
    string_to_static_str(out)
}

/* Command line entrypoint */

fn main() {
    /* Create experiment-specific subcommands */

    let command_vb = SubCommand::with_name("vb")
        .about("Custom value-barrier experiment")
        .arg(
            Arg::with_name("PAR")
                .index(1)
                .required(true)
                .help("parallelism (1 = sequential)"),
        )
        .arg(
            Arg::with_name("VAL_RATE")
                .index(2)
                .required(true)
                .help("rate of values in events/ms"),
        )
        .arg(
            Arg::with_name("VALS_PER_HB")
                .index(3)
                .required(true)
                .help("values per heartbeat (per worker)"),
        )
        .arg(
            Arg::with_name("HBS_PER_BAR")
                .index(4)
                .required(true)
                .help("heartbeats per barrier"),
        )
        .arg(
            Arg::with_name("DURATION")
                .index(5)
                .required(true)
                .help("experiment duration in seconds"),
        )
        .arg(
            Arg::with_name("gen-only")
                .short("g")
                .help("data generation only (no processing)"),
        );
    let command_vb_exp1 = SubCommand::with_name("vbe1").about(
        "Value-barrier experiment 1: vary the parallelism and input rate",
    );
    let command_pv = SubCommand::with_name("pv")
        .about("Custom pageview experiment")
        .arg(
            Arg::with_name("PAR")
                .index(1)
                .required(true)
                .help("parallelism (1 = sequential)"),
        )
        .arg(
            Arg::with_name("RATE")
                .index(2)
                .required(true)
                .help("events per ms overall"),
        )
        .arg(
            Arg::with_name("PAGE_RATIO")
                .index(3)
                .required(true)
                .help("ratio of page #0 to page #1"),
        )
        .arg(
            Arg::with_name("VIEW_RATIO")
                .index(4)
                .required(true)
                .help("ratio of views to updates"),
        )
        .arg(
            Arg::with_name("DURATION")
                .index(5)
                .required(true)
                .help("experiment duration in seconds"),
        )
        .arg(
            Arg::with_name("gen-only")
                .short("g")
                .help("data generation only (no processing)"),
        );
    let command_pv_exp1 = SubCommand::with_name("pve1")
        .about("Pageview experiment 1: vary the parallelism and input rate");

    /* Create application */

    let app = App::new("naiad-experiment")
        .author("Caleb Stanford")
        .about("Command line for running Naiad (Timely Dataflow) experiments")
        .setting(AppSettings::SubcommandRequired)
        .subcommand(command_vb)
        .subcommand(command_vb_exp1)
        .subcommand(command_pv)
        .subcommand(command_pv_exp1);

    /* Parse arguments depending on subcommand */

    let matches = app.clone().get_matches();

    if let Some(matches) = matches.subcommand_matches("vb") {
        /* Value Barrier Command */

        let arg1 = matches.value_of("PAR").unwrap();
        let arg2 = matches.value_of("VAL_RATE").unwrap();
        let arg3 = matches.value_of("VALS_PER_HB").unwrap();
        let arg4 = matches.value_of("HBS_PER_BAR").unwrap();
        let arg5 = matches.value_of("DURATION").unwrap();
        let params = VBExperimentParams {
            parallelism: arg1.parse().expect("expected u64"),
            val_rate_per_milli: arg2.parse().expect("expected u64 (events/ms)"),
            vals_per_hb_per_worker: arg3.parse().expect("expected u64"),
            hbs_per_bar: arg4.parse().expect("expected u64"),
            exp_duration_secs: arg5.parse().expect("expected u64 (secs)"),
        };
        let args = [arg1, arg2, arg3, arg4, arg5];

        if matches.is_present("gen-only") {
            let results_path = make_results_path("vbgen", &args);
            params.run_vb_experiment_gen_only(results_path);
        } else {
            let results_path = make_results_path("vb", &args);
            params.run_vb_experiment_main(results_path);
        }
    } else if let Some(_matches) = matches.subcommand_matches("vbe1") {
        /* Value Barrier Experiment 1 */

        let mut params = VBExperimentParams {
            parallelism: 0,        // will be set
            val_rate_per_milli: 0, // will be set
            vals_per_hb_per_worker: 100,
            hbs_per_bar: 100,
            exp_duration_secs: 5,
        };
        let val_rates = &[1, 5, 10, 50, 100, 150, 200, 250, 300, 350, 400];
        let parallelisms = &[1, 2, 4, 8];
        for &par in parallelisms {
            params.parallelism = par;
            println!("===== Parallelism: {} =====", par);
            let results_path = make_results_path(
                "vbe1",
                &[&("par".to_owned() + &par.to_string())],
            );
            for &val_rate in val_rates {
                params.val_rate_per_milli = val_rate;
                println!("=== Value rate (events/ms): {} ===", val_rate);
                params.run_vb_experiment_main(results_path);
            }
        }
    } else if let Some(matches) = matches.subcommand_matches("pv") {
        /* Pageview Command */

        let arg1 = matches.value_of("PAR").unwrap();
        let arg2 = matches.value_of("RATE").unwrap();
        let arg3 = matches.value_of("PAGE_RATIO").unwrap();
        let arg4 = matches.value_of("VIEW_RATIO").unwrap();
        let arg5 = matches.value_of("DURATION").unwrap();
        let params = PVExperimentParams {
            parallelism: arg1.parse().expect("expected u64"),
            events_per_milli: arg2.parse().expect("expected u64 (events/ms)"),
            page0_per_page1: arg3.parse().expect("expected u64"),
            views_per_update: arg4.parse().expect("expected u64"),
            exp_duration_secs: arg5.parse().expect("expected u64 (secs)"),
        };
        let args = [arg1, arg2, arg3, arg4, arg5];

        if matches.is_present("gen-only") {
            let results_path = make_results_path("pvgen", &args);
            params.run_pv_experiment_gen_only(results_path);
        } else {
            let results_path = make_results_path("pv", &args);
            params.run_pv_experiment_main(results_path);
        }
    } else if let Some(_matches) = matches.subcommand_matches("pve1") {
        /* Pageview Experiment 1 */

        let mut params = PVExperimentParams {
            parallelism: 0,      // will be set
            events_per_milli: 0, // will be set
            page0_per_page1: 100,
            views_per_update: 100,
            exp_duration_secs: 5,
        };
        let rates = &[1, 5, 10, 50, 100, 150, 200, 250, 300, 350, 400];
        let parallelisms = &[1, 2, 4, 8];
        for &par in parallelisms {
            params.parallelism = par;
            println!("===== Parallelism: {} =====", par);
            let results_path = make_results_path(
                "pve1",
                &[&("par".to_owned() + &par.to_string())],
            );
            for &rate in rates {
                params.events_per_milli = rate;
                println!("=== Input rate (events/ms): {} ===", rate);
                params.run_pv_experiment_main(results_path);
            }
        }
    } else {
        unreachable!();
    }
}
