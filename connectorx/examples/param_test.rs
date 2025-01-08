use num_cpus;
use sysinfo;

fn main() {
    let logical_core_count = num_cpus::get();
    println!("Number of logical CPU cores: {}", logical_core_count);
    source.idle_threads = sysinfo::get_idle_threads();
}