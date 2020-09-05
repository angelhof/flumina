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

fn value_source<G>(scope: &G) -> Stream<G, i64>
where
    G: Scope<Timestamp = i64>
{
    source(scope, "Source", |capability: Capability<<G as timely::dataflow::scopes::ScopeParent>::Timestamp>, info: OperatorInfo| {

        // Acquire a re-activator for this operator.
        let activator = scope.activator_for(&info.address[..]);

        // Wrap capability for when we are done with it
        let mut cap = Some(capability);

        // Return closure
        move |output: &mut OutputHandle<i64, i64, Tee<i64, i64>>| {
            let mut done = false;
            if let Some(cap) = cap.as_mut() {
                // get some data and send it.
                let time = cap.time().clone();
                output.session(&cap)
                      .give(*cap.time());
                // downgrade capability.
                cap.downgrade(&(time + 1));
                done = time > 20;
            }
            if done { cap = None; }
            else    { activator.activate(); }
        }

    })
}

/*
Main function so that the generator can be run as a standalone
binary without computation if desired
*/
fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        let w_index = worker.index();
        worker.dataflow(|scope| {
            value_source(scope)
            .inspect(|x| println!("item generated: {:?}", x));
        });
    }).unwrap();
}
