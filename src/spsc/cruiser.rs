// -----------------------------------------------------------------------------
// Cruiser wrapper – concurrent monitor stub (optional)
// -----------------------------------------------------------------------------
mod cruiser {
   // Placeholder – implementing full heap‑integrity monitor
}

// -----------------------------------------------------------------------------
// Optional Criterion bench (enable with `--features bench`)
// -----------------------------------------------------------------------------
#[cfg(feature="bench")]
mod benches {
   use super::*;
   use criterion::{criterion_group,criterion_main,Criterion,Throughput,black_box};
   use std::thread;
   use std::time::Duration;

   fn bench_queue<Q: SpscQueue<usize> + 'static>(c:&mut Criterion,name:&str,make:impl Fn()->Q+Send+'static){
      c.benchmark_group(name).throughput(Throughput::Elements(1_000_000)).bench_function("latency",|b|{
         b.iter_custom(|iters|{
            let q=make();
            let prod=thread::spawn(move||{ for i in 0..iters{ let _=q.push(i as usize);} });
            let start=std::time::Instant::now();
            let mut got=0;
            while got<iters{ if q.pop().is_ok(){ got+=1; }}
            prod.join().unwrap();
            start.elapsed()
         })
      });
   }
   pub fn benches(c:&mut Criterion){
      bench_queue(c,"lamport",||LamportQueue::with_capacity(1024));
   }
   criterion_group!(benches_grp,benches);
   criterion_main!(benches_grp);
}