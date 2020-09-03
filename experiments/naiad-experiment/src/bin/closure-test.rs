
use std::vec::Vec;

fn main() {
    let val = 3;
    let mut val_mut = 3;
    let vec = vec![0; 5];
    let mut vec_mut = Vec::new();
    vec_mut.push(0);

    let mut do_something = |x: i64| {
        // val += x; // error: val is immutable
        val_mut += x;
        // vec.push(x); // error: vec is immutable
        vec_mut.push(x);
        println!("input: {}", x);
        println!("  val: {}", val);
        println!("  val_mut: {}", val_mut);
        println!("  vec: {:?}", vec);
        println!("  vec_mut: {:?}", vec_mut);
    };

    do_something(3);
    do_something(2);
    do_something(3);
    do_something(1);

}
