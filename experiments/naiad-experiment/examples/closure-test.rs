use std::vec::Vec;
use std::cell::RefCell;
use std::rc::Rc;

// Useful for debugging
fn print_type_of<T>(_: &T) {
    println!("{}", std::any::type_name::<T>())
}

fn main() {
    let val_raw = 0;
    let mut val = Rc::new(RefCell::new(val_raw));
    let mut vec = Rc::new(RefCell::new(Vec::new()));

    vec.borrow_mut().push(0);

    let val_2 = val.clone();
    let vec_2 = vec.clone();
    let mut do_something = move |x: i64| {
        *val_2.borrow_mut() += x;
        vec_2.borrow_mut().push(x);
        println!("input: {}", x);
        println!("  val: {}", val_2.borrow());
        println!("  vec: {:?}", vec_2.borrow());
    };

    do_something(3);
    do_something(2);

    *val.borrow_mut() += 100;
    vec.borrow_mut().push(100);

    do_something(3);
    do_something(1);

}
