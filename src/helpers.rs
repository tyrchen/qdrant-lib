use segment::common::cpu::get_num_cpus;
use std::cmp::max;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::runtime::{self, Runtime};

pub fn create_search_runtime(max_search_threads: usize) -> io::Result<Runtime> {
    let mut search_threads = max_search_threads;

    if search_threads == 0 {
        let num_cpu = get_num_cpus();
        // At least one thread, but not more than number of CPUs - 1 if there are more than 2 CPU
        // Example:
        // Num CPU = 1 -> 1 thread
        // Num CPU = 2 -> 2 thread - if we use one thread with 2 cpus, its too much un-utilized resources
        // Num CPU = 3 -> 2 thread
        // Num CPU = 4 -> 3 thread
        // Num CPU = 5 -> 4 thread
        search_threads = match num_cpu {
            0 => 1,
            1 => 1,
            2 => 2,
            _ => num_cpu - 1,
        };
    }

    runtime::Builder::new_multi_thread()
        .worker_threads(search_threads)
        .max_blocking_threads(search_threads)
        .enable_all()
        .thread_name_fn(|| {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
            format!("search-{id}")
        })
        .build()
}

pub fn create_update_runtime(max_optimization_threads: usize) -> io::Result<Runtime> {
    let mut update_runtime_builder = runtime::Builder::new_multi_thread();

    update_runtime_builder
        .enable_time()
        .thread_name_fn(move || {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let update_id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
            format!("update-{update_id}")
        });

    if max_optimization_threads > 0 {
        // panics if val is not larger than 0.
        update_runtime_builder.max_blocking_threads(max_optimization_threads);
    }
    update_runtime_builder.build()
}

pub fn create_general_purpose_runtime() -> io::Result<Runtime> {
    runtime::Builder::new_multi_thread()
        .enable_time()
        .enable_io()
        .worker_threads(max(get_num_cpus(), 2))
        .thread_name_fn(|| {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let general_id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
            format!("general-{general_id}")
        })
        .build()
}
