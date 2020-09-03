use std::vec::Vec;

fn main() {
    let val = 3;
    let vec = vec![0; 5];

    let mut val_mut = 3;
    let mut vec_mut = Vec::new();

    vec_mut.push(0);

    let mut do_something = move |x: i64| {
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

    // Doesn't work (whether or not the closure uses move)
    // Problem is that the closure takes ownership of mutable variables
    // (or borrows mutably), preventing further mutation
    // val_mut += 3;
    // vec_mut.push(3);

    do_something(3);
    do_something(1);

}
