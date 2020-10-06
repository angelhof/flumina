extern crate timely;

use timely::dataflow::operators::*;

use std::string::String;
use std::vec::Vec;

const HOSTFILE: &str = "hosts/ec2_hosts_test.txt";

fn run_dataflow(mut args: Vec<&str>) {
    let args_iter = args.drain(..).map(|s| String::from(s));
    timely::execute_from_args(args_iter, |worker| {
        let w_index = worker.index();
        let w_total = worker.peers();
        println!("worker {}/{} start", w_index, w_total);
        worker.dataflow::<(), _, _>(|scope| {
            (0..10).to_stream(scope).inspect(move |x| {
                println!("worker {}/{} seen: {:?}", w_index, w_total, x)
            });
        });
        println!("worker {}/{} complete", w_index, w_total);
    })
    .unwrap();
}

fn main() {
    let args1 = vec!["-w", "1", "-n", "1", "-p", "0", "-h", HOSTFILE];
    let args2 = vec!["-w", "1", "-n", "2", "-p", "0", "-h", HOSTFILE];
    run_dataflow(args1);
    run_dataflow(args2);
}
