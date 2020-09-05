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

        // Internal details of timely dataflow
        // 1. Acquire a re-activator for this operator.
        // 2. Wrap capability in an option so that we can release it when
        //    done by setting it to None
        let activator = scope.activator_for(&info.address[..]);
        let mut maybe_cap = Some(capability);

        // Return closure
        move |output: &mut OutputHandle<i64, i64, Tee<i64, i64>>| {
            if let Some(cap) = maybe_cap.as_mut() {
                // get some data and send it.
                let time = cap.time().clone();
                output.session(&cap)
                      .give(*cap.time());
                // downgrade capability.
                cap.downgrade(&(time + 1));
                if time > 20 {
                    maybe_cap = None;
                }
                else {
                    activator.activate();
                }
            }
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
