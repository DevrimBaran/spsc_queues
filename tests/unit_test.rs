// spsc_queues/tests/unit_test.rs

use spsc_queues::DehnaviQueue; 
use spsc_queues::SpscQueue;    
use spsc_queues::spsc::PopError; // Ensure this path is valid based on your exports


use std::sync::atomic::Ordering; 
use std::sync::Arc;
use std::thread;
use std::time::Duration;


#[test]
fn test_dehnavi_new_queue_effective_capacity() {
   // DehnaviQueue now requires capacity > 1.
   // k=2 means it holds 1 item.
   // k=3 means it holds 2 items.
   let q_cap_is_3 = DehnaviQueue::<i32>::new(3); 
   assert_eq!(q_cap_is_3.capacity, 3); 
   q_cap_is_3.push(1).unwrap();
   q_cap_is_3.push(2).unwrap(); 
   assert!(!q_cap_is_3.available(), "Queue with k=3 should be full after 2 items");

   let q_cap_is_2 = DehnaviQueue::<i32>::new(2); 
   assert_eq!(q_cap_is_2.capacity, 2);
   q_cap_is_2.push(1).unwrap();
   assert!(!q_cap_is_2.available(), "Queue with k=2 should be full after 1 item");

   // The k=1 case is removed as DehnaviQueue::new(1) should panic.
   // See test_dehnavi_new_with_capacity_1_panics below.
}

#[test]
#[should_panic(expected = "Capacity (k) must be greater than 1 for DehnaviQueue.")]
fn test_dehnavi_new_with_capacity_1_panics() {
   // This test verifies that creating a queue with capacity 1 panics.
   let _q = DehnaviQueue::<i32>::new(1);
}


#[test]
fn test_dehnavi_single_thread_push_pop_exact_capacity() {
   let q = DehnaviQueue::new(3); // Holds 2 items effectively
   
   q.push(10).unwrap(); 
   q.push(20).unwrap(); 
   
   assert!(!q.available());
   assert!(!q.empty());

   assert_eq!(q.pop().unwrap(), 10); 
   assert!(q.available());
   assert!(!q.empty());

   assert_eq!(q.pop().unwrap(), 20); 
   assert!(q.available()); 
   assert!(q.empty());

   match q.pop() {
      Err(PopError {}) => { /* Expected */ } 
      Ok(v) => panic!("Should be empty, got {:?}", v),
   }
}

#[test]
fn test_dehnavi_overwrite_logic() {
   let q = DehnaviQueue::new(3); // k=3, holds 2 items

   q.push(10).unwrap(); 
   q.push(20).unwrap(); 
                        
   q.push(30).unwrap(); // Overwrites 10
   
   assert_eq!(q.wc.load(Ordering::Relaxed), 0);
   assert_eq!(q.rc.load(Ordering::Relaxed), 1);

   assert_eq!(q.pop().unwrap(), 20); 
   assert_eq!(q.pop().unwrap(), 30); 
   assert!(q.pop().is_err());
   assert!(q.empty());
}

#[test]
fn test_dehnavi_spsc_sequential_no_overwrite() {
   let q = Arc::new(DehnaviQueue::<i32>::new(100)); // Holds 99 items
   let q_producer = q.clone();
   let q_consumer = q.clone();
   let num_items = 50; 

   let producer_thread = thread::spawn(move || {
      for i in 0..num_items { 
         q_producer.push(i as i32).unwrap(); 
      }
   });

   let consumer_thread = thread::spawn(move || {
      for i in 0..num_items { 
         loop {
               match q_consumer.pop() {
                  Ok(val) => { 
                     assert_eq!(val, i as i32); 
                     break;
                  }
                  Err(PopError {}) => { 
                     thread::yield_now();
                  }
               }
         }
      }
   });

   producer_thread.join().unwrap();
   consumer_thread.join().unwrap();
   assert!(q.empty());
}

#[test]
fn test_dehnavi_spsc_with_overwrites_consumer_slow() {
   let q_capacity_k: usize = 5; // Holds 4 items
   let q = Arc::new(DehnaviQueue::<i32>::new(q_capacity_k));
   let q_producer = q.clone();
   let q_consumer = q.clone();
   let num_produce: usize = 20;
   
   // Removed unused variable: received_items_vec 

   let producer_handle = thread::spawn(move || { 
      for i in 0..num_produce { 
         q_producer.push(i as i32).unwrap(); 
         if i < 5 { 
               thread::sleep(Duration::from_millis(1));
         } else if i % 4 == 0 { 
               thread::yield_now();
         }
      }
   });
   
   thread::sleep(Duration::from_millis(10)); 

   let consumer_handle = thread::spawn(move || { 
      let mut received_items_inner_vec: Vec<i32> = Vec::new();
      let mut attempts: usize = 0;
      let max_attempts: usize = num_produce * 3; 

      while received_items_inner_vec.len() < num_produce && attempts < max_attempts {
         match q_consumer.pop() {
               Ok(val) => { 
                  received_items_inner_vec.push(val);
                  if received_items_inner_vec.len() % 2 == 0 {
                     thread::sleep(Duration::from_millis(1));
                  } else {
                     thread::yield_now();
                  }
               }
               Err(PopError {}) => { 
                  thread::yield_now(); 
               }
         }
         attempts += 1;
      }
      received_items_inner_vec
   });

   producer_handle.join().unwrap(); 
   let final_received = consumer_handle.join().unwrap();

   let mut remaining_after_join: Vec<i32> = Vec::new(); 
   while let Ok(item) = q.pop() { 
      remaining_after_join.push(item);
   }
   
   assert!(q.empty(), "Queue should be empty after all operations");
   
   let mut all_received = final_received;
   all_received.extend(remaining_after_join);

   if all_received.is_empty() && num_produce > 0 {
      panic!("No items received in overwrite test, which is unexpected if items were produced.");
   }
   
   println!("[Overwrite Test] Produced: {}, Received: {} items: {:?}", num_produce, all_received.len(), all_received);

   for i in 0..(all_received.len().saturating_sub(1)) {
      assert!(all_received[i] < all_received[i+1], "Received items not strictly increasing: {:?} at index {}", all_received, i);
   }
   
   if num_produce > 0 {
      assert!(!all_received.is_empty(), "Should have received at least one item if items were produced.");
   }

   let effective_capacity: usize = q_capacity_k - 1; 
   if num_produce > effective_capacity && effective_capacity > 0 { 
      assert!(all_received.len() < num_produce || all_received.len() == effective_capacity,
               "Expected fewer items ({}) than produced ({}) due to overwrites, or exactly effective capacity ({}). Got: {:?}",
               all_received.len(), num_produce, effective_capacity, all_received);
   } else if num_produce > 0 { 
      let expected_len: usize = num_produce.min(effective_capacity);
      if expected_len > 0 {
         assert_eq!(all_received.len(), expected_len,
                     "Received count mismatch. Expected {}, got {}. All received: {:?}",
                     expected_len, all_received.len(), all_received);
      } else if num_produce > 0 && expected_len == 0 { // Should not be hit if k > 1
            assert!(all_received.is_empty(), "Expected 0 items if effective capacity is 0, got: {:?}", all_received);
      }
   }

   if let Some(&last_val) = all_received.last() { 
      assert!((last_val as usize) < num_produce, "Last value {:?} (as usize) should be < num_produce {}", last_val, num_produce);
      
      if !all_received.is_empty() && effective_capacity > 0 {
         let first_val = all_received[0]; 
         assert!((first_val as usize) >= num_produce.saturating_sub(effective_capacity + (all_received.len().saturating_sub(1))),
                  "First received item {} (as usize) seems too old. Num_produce: {}, effective_cap: {}, received_len: {}. All: {:?}",
                  first_val, num_produce, effective_capacity, all_received.len(), all_received);
      }
   }
}
