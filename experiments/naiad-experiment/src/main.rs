/*
    Command-line entrypoint to run experiments.
*/

use naiad_experiment::ec2::get_ec2_node_number;
use naiad_experiment::experiment::TimelyParallelism;
use naiad_experiment::pageview::PVExperimentParams;
use naiad_experiment::util::{
    current_datetime_str, sleep_for_secs, string_to_static_str,
};
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

#[rustfmt::skip]
fn main() {
    /* Create experiment-specific subcommands */

    let command_vb = SubCommand::with_name("vb")
        .about("Custom value-barrier experiment")
        .arg(Arg::with_name("PAR_WKRS").index(1).required(true)
            .help("# workers (1 = sequential)"))
        .arg(Arg::with_name("PAR_NODES").index(2).required(true)
            .help("# nodes (1 = single node) \
                   (requires hostnames file to be up-to-date)"))
        .arg(Arg::with_name("VAL_RATE").index(3).required(true)
            .help("rate of values in events/ms"))
        .arg(Arg::with_name("VALS_PER_HB").index(4).required(true)
            .help("values per heartbeat (per worker)"))
        .arg(Arg::with_name("HBS_PER_BAR").index(5).required(true)
            .help("heartbeats per barrier"))
        .arg(Arg::with_name("DURATION").index(6).required(true)
            .help("experiment duration in seconds"))
        .arg(Arg::with_name("gen-only").short("g")
            .help("data generation only (no processing)"))
        .arg(Arg::with_name("fraud-detection").short("f")
            .help("instead of running the simple value-barrier example, run\
                   the full fraud detection example."));
    let command_pv = SubCommand::with_name("pv")
        .about("Custom pageview experiment")
        .arg(Arg::with_name("PAR_WKRS").index(1).required(true)
            .help("# workers (1 = sequential)"))
        .arg(Arg::with_name("PAR_NODES").index(2).required(true)
            .help("# nodes (1 = single node) \
                   (requires hostnames file to be up-to-date)"))
        .arg(Arg::with_name("RATE").index(3).required(true)
            .help("events per ms overall"))
        .arg(Arg::with_name("VIEW_RATIO").index(4).required(true)
            .help("ratio of views to updates"))
        .arg(Arg::with_name("DURATION").index(5).required(true)
            .help("experiment duration in seconds"))
        .arg(Arg::with_name("gen-only").short("g")
            .help("data generation only (no processing)"));
    let command_exp1 = SubCommand::with_name("exp1")
        .about("Experiment 1: \
                For value-barrier, vary the parallelism and input rate");
    let command_exp2 = SubCommand::with_name("exp2")
        .about("Experiment 2: \
                For page-view, vary the parallelism and input rate");
    let command_exp3 = SubCommand::with_name("exp3")
        .about("Experiment 3: \
                For the fraud-detection variant of value-barrier, \
                vary the parallelism and input rate");

    /* Create application */

    let app = App::new("naiad-experiment")
        .author("Caleb Stanford")
        .about("Command line for running Naiad (Timely Dataflow) experiments")
        .setting(AppSettings::SubcommandRequired)
        .subcommand(command_vb)
        .subcommand(command_pv)
        .subcommand(command_exp1)
        .subcommand(command_exp2)
        .subcommand(command_exp3);

    /* Parse arguments depending on subcommand */

    let matches = app.clone().get_matches();

    if let Some(matches) = matches.subcommand_matches("vb") {
        /* Value Barrier Command */
        let arg1 = matches.value_of("PAR_WKRS").unwrap();
        let arg2 = matches.value_of("PAR_NODES").unwrap();
        let arg3 = matches.value_of("VAL_RATE").unwrap();
        let arg4 = matches.value_of("VALS_PER_HB").unwrap();
        let arg5 = matches.value_of("HBS_PER_BAR").unwrap();
        let arg6 = matches.value_of("DURATION").unwrap();
        let params = VBExperimentParams {
            parallelism: TimelyParallelism::new_for_ec2(
                arg1.parse().expect("expected u64"),
                arg2.parse().expect("expected u64"),
            ),
            val_rate_per_milli: arg3.parse().expect("expected u64 (events/ms)"),
            vals_per_hb_per_worker: arg4.parse().expect("expected u64"),
            hbs_per_bar: arg5.parse().expect("expected u64"),
            exp_duration_secs: arg6.parse().expect("expected u64 (secs)"),
        };
        let args = [arg1, arg2, arg3, arg4, arg5, arg6];

        if matches.is_present("gen-only") {
            let results_path = make_results_path("vbgen", &args);
            params.run_vb_experiment_gen_only(results_path);
        } else if matches.is_present("fraud-detection") {
            let results_path = make_results_path("fd", &args);
            params.run_fd_experiment(results_path);
        } else {
            let results_path = make_results_path("vb", &args);
            params.run_vb_experiment_main(results_path);
        }
    } else if let Some(matches) = matches.subcommand_matches("pv") {
        /* Pageview Command */
        let arg1 = matches.value_of("PAR_WKRS").unwrap();
        let arg2 = matches.value_of("PAR_NODES").unwrap();
        let arg3 = matches.value_of("RATE").unwrap();
        let arg4 = matches.value_of("VIEW_RATIO").unwrap();
        let arg5 = matches.value_of("DURATION").unwrap();
        let params = PVExperimentParams {
            parallelism: TimelyParallelism::new_for_ec2(
                arg1.parse().expect("expected u64"),
                arg2.parse().expect("expected u64"),
            ),
            views_per_milli: arg3.parse().expect("expected u64 (events/ms)"),
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
    } else if let Some(_matches) = matches.subcommand_matches("exp1") {
        /* Experiment 1: Value Barrier */
        let mut params = VBExperimentParams {
            parallelism: TimelyParallelism::new_sequential(), // will be set
            val_rate_per_milli: 0,                            // will be set
            vals_per_hb_per_worker: 100,
            hbs_per_bar: 100,
            exp_duration_secs: 5,
        };
        let val_rates = &[
            50, 100, 150, 200, 250, 300, 350, 400, 450, 500, 550, 600,
        ];
        let par_workers = &[1, 2];
        let par_nodes = &[1, 2, 4]; // , 8, 12, 16, 20];
        for &par_w in par_workers {
            for &par_n in par_nodes {
                // Only run experiment if this node # is used in the experiment
                if get_ec2_node_number() >= par_n {
                    println!("Sleeping for {}", params.exp_duration_secs);
                    sleep_for_secs(params.exp_duration_secs);
                } else {
                    params.parallelism = TimelyParallelism::new_for_ec2(par_w, par_n);
                    println!("===== Parallelism: {} =====", params.parallelism.to_csv());
                    let results_path = make_results_path(
                        "exp1_vb",
                        &[ &("w".to_owned() + &par_w.to_string()),
                           &("n".to_owned() + &par_n.to_string()), ],
                    );
                    for &val_rate in val_rates {
                        params.val_rate_per_milli = val_rate;
                        println!("=== Value rate (events/ms): {} ===", val_rate);
                        params.run_vb_experiment_main(results_path);
                    }
                }
            }
        }
    } else if let Some(_matches) = matches.subcommand_matches("exp2") {
        /* Experiment 2: Pageview */
        let mut params = PVExperimentParams {
            parallelism: TimelyParallelism::new_sequential(), // will be set
            views_per_milli: 0,                               // will be set
            views_per_update: 10000,
            exp_duration_secs: 5,
        };
        let rates = &[
            50, 100, 150, 200, 250, 300, 350, 400, 450, 500, 550, 600,
        ];
        let par_workers = &[1, 2];
        let par_nodes = &[1, 2, 4]; // , 8, 12, 16, 20];
        for &par_w in par_workers {
            for &par_n in par_nodes {
                // Only run experiment if this node # is used in the experiment
                if get_ec2_node_number() >= par_n {
                    println!("Sleeping for {}", params.exp_duration_secs);
                    sleep_for_secs(params.exp_duration_secs);
                } else {
                    params.parallelism = TimelyParallelism::new_for_ec2(par_w, par_n);
                    println!("===== Parallelism: {} =====", params.parallelism.to_csv());
                    let results_path = make_results_path(
                        "exp2_pv",
                        &[ &("w".to_owned() + &par_w.to_string()),
                           &("n".to_owned() + &par_n.to_string()), ],
                    );
                    for &rate in rates {
                        params.views_per_milli = rate;
                        println!("=== Input rate (events/ms): {} ===", rate);
                        params.run_pv_experiment_main(results_path);
                    }
                }
            }
        }
    } else if let Some(_matches) = matches.subcommand_matches("exp3") {
        /*  Experiment 3: Fraud Detection */
        let mut params = VBExperimentParams {
            parallelism: TimelyParallelism::new_sequential(), // will be set
            val_rate_per_milli: 0,                            // will be set
            vals_per_hb_per_worker: 100,
            hbs_per_bar: 100,
            exp_duration_secs: 5,
        };
        let val_rates = &[
            50, 100, 150, 200, 250, 300, 350, 400, 450, 500, 550, 600,
        ];
        let par_workers = &[1, 2];
        let par_nodes = &[1, 2, 4]; // , 8, 12, 16, 20];
        for &par_w in par_workers {
            for &par_n in par_nodes {
                // Only run experiment if this node # is used in the experiment
                if get_ec2_node_number() >= par_n {
                    println!("Sleeping for {}", params.exp_duration_secs);
                    sleep_for_secs(params.exp_duration_secs);
                } else {
                    params.parallelism = TimelyParallelism::new_for_ec2(par_w, par_n);
                    println!("===== Parallelism: {} =====", params.parallelism.to_csv());
                    let results_path = make_results_path(
                        "exp3_fd",
                        &[ &("w".to_owned() + &par_w.to_string()),
                           &("n".to_owned() + &par_n.to_string()), ],
                    );
                    for &val_rate in val_rates {
                        params.val_rate_per_milli = val_rate;
                        println!("=== Value rate (events/ms): {} ===", val_rate);
                        params.run_fd_experiment(results_path);
                    }
                }
            }
        }
    } else {
        unreachable!();
    }
}
