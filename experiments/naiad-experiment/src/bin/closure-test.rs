use std::vec::Vec;

fn main() {
    let mut val = 3;
    let mut vec = Vec::new();

    vec.push(0);

    let mut do_something = move |x: i64| {
        val += x;
        vec.push(x);
        println!("input: {}", x);
        println!("  val: {}", val);
        println!("  vec: {:?}", vec);
    };

    do_something(3);
    do_something(2);

    // Doesn't work (whether or not the closure uses move)
    // Problem is that the closure takes ownership of mutable variables
    // (or borrows mutably), preventing further mutation
    // val += 3;
    // vec.push(3);

    do_something(3);
    do_something(1);

}
