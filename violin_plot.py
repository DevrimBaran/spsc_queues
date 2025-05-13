import json
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# --- Configuration ---
CRITERION_BASE_PATH = './target/criterion/' 
BENCHMARK_FILE_STEM = '' # Corrected based on your path structure

# IMPORTANT: Update these to the EXACT FOLDER NAMES in target/criterion/
BENCHMARK_FUNCTION_IDS = [
   "Lamport",
   "B-Queue",
   "mSPSC",
   "uSPSC",
   "dSPSC",
   "Dehnavi",
   "Iffq",
   "Biffq",
   "FFq"
]

OUTPUT_PLOT_FILE = 'spsc_queue_performance_violin.png'
PLOT_TITLE = 'Performance Comparison for IPC SPSC Queues via shared memory'
Y_AXIS_LABEL = 'Execution Time per Iteration (microseconds)'

# --- Script ---

def load_benchmark_data(base_path, benchmark_file_stem, function_id_folder_name):
   """
   Loads raw sample times from Criterion output.
   Prioritizes 'new/sample.json' and extracts the 'times' array.
   Falls back to trying 'new/estimates.json' if it's a direct list of times.
   Assumes times are in nanoseconds.
   """
   if benchmark_file_stem:
      path_segment = os.path.join(base_path, benchmark_file_stem, function_id_folder_name)
   else:
      path_segment = os.path.join(base_path, function_id_folder_name)

   # Primary target: new/sample.json
   sample_json_path_new = os.path.join(path_segment, 'new', 'sample.json')

   data_file_to_try = None
   is_sample_json = False

   if os.path.exists(sample_json_path_new):
      data_file_to_try = sample_json_path_new
      is_sample_json = True
   else:
      print(f"Warning: Could not find 'sample.json' or 'estimates.json' for '{function_id_folder_name}' (stem: '{benchmark_file_stem}').")
      return None

   try:
      with open(data_file_to_try, 'r') as f:
         data = json.load(f)
      
      if is_sample_json:
         if isinstance(data, dict) and 'times' in data and isinstance(data['times'], list):
               if all(isinstance(x, (int, float)) for x in data['times']):
                  return np.array(data['times'])
               else:
                  print(f"Warning: 'times' array in '{data_file_to_try}' contains non-numeric data.")
                  return None
         else:
               print(f"Warning: Expected 'times' array in '{data_file_to_try}', but not found or not a list. Keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
               return None
      else: # It's an estimates.json file, check if it's a direct list of samples
         if isinstance(data, list) and all(isinstance(x, (int, float)) for x in data):
               return np.array(data)
         else:
               print(f"Warning: '{data_file_to_try}' is not a direct list of samples. Content type: {type(data)}")
               if isinstance(data, dict):
                  print(f"  Available keys: {list(data.keys())}") # This is what you were seeing
               return None

   except FileNotFoundError: # Should be caught by os.path.exists
      print(f"Warning: File not found (unexpected): '{data_file_to_try}'")
      return None
   except json.JSONDecodeError:
      print(f"Warning: Could not decode JSON for '{function_id_folder_name}' at '{data_file_to_try}'")
      return None
   except Exception as e:
      print(f"Warning: Error loading data for '{function_id_folder_name}' from '{data_file_to_try}': {e}")
      return None

def main():
   all_data = []
   benchmark_labels_for_plot = []

   print(f"Looking for benchmark data in base path: {os.path.abspath(CRITERION_BASE_PATH)}")
   if BENCHMARK_FILE_STEM:
      print(f"Using benchmark file stem: {BENCHMARK_FILE_STEM}")
   else:
      print("Assuming benchmark function folders are directly under the base path.")


   for bench_func_id_from_config in BENCHMARK_FUNCTION_IDS:
      print(f"\nProcessing configured Benchmark ID: {bench_func_id_from_config}")
      
      folder_to_try = bench_func_id_from_config 
      
      samples_ns = load_benchmark_data(CRITERION_BASE_PATH, BENCHMARK_FILE_STEM, folder_to_try)
         
      if samples_ns is not None and len(samples_ns) > 0:
         print(f"  Successfully loaded {len(samples_ns)} samples for ID '{bench_func_id_from_config}' (from folder '{folder_to_try}')")
         samples_us = samples_ns / 1000.0 
         all_data.append(samples_us)
         benchmark_labels_for_plot.append(bench_func_id_from_config) 
      else:
         print(f"  Could not load valid data for benchmark ID: {bench_func_id_from_config}. Ensure this exact folder name exists: '{folder_to_try}' directly under '{CRITERION_BASE_PATH}{BENCHMARK_FILE_STEM}'.")

   if not all_data:
      print("\nError: No benchmark data found or loaded. Cannot generate plot.")
      print("Please ensure:")
      print(f"1. You have run 'cargo bench --bench <your_bench_file_name_in_Cargo.toml>' (e.g., 'cargo bench --bench process_bench' if that's your bench file).")
      print(f"2. CRITERION_BASE_PATH ('{CRITERION_BASE_PATH}') is correct.")
      if BENCHMARK_FILE_STEM:
            print(f"3. BENCHMARK_FILE_STEM ('{BENCHMARK_FILE_STEM}') matches the directory name under '{CRITERION_BASE_PATH}'.")
            print(f"4. BENCHMARK_FUNCTION_IDS list contains the **EXACT FOLDER NAMES** found under '{CRITERION_BASE_PATH}/{BENCHMARK_FILE_STEM}/'.")
      else:
            print(f"3. BENCHMARK_FUNCTION_IDS list contains the **EXACT FOLDER NAMES** found directly under '{CRITERION_BASE_PATH}/'.")
      print(f"5. Criterion output files (sample.json or estimates.json containing a list of numbers) exist in 'new/' subdirectories (or directly in the function ID folder).")
      return

   plot_data_list = []
   for label, data_array in zip(benchmark_labels_for_plot, all_data):
      for value in data_array:
         plot_data_list.append({'Queue Type': label, Y_AXIS_LABEL: value})
   
   if not plot_data_list:
      print("\nError: No data prepared for DataFrame. Cannot generate plot.")
      return
      
   df = pd.DataFrame(plot_data_list)

   if df.empty:
      print("\nError: DataFrame is empty. No data to plot.")
      return

   plt.figure(figsize=(16, 9)) 
   sns.violinplot(x='Queue Type', y=Y_AXIS_LABEL, data=df, palette='viridis', cut=0, inner='quartile', scale='width')
   
   plt.title(PLOT_TITLE, fontsize=18, pad=20)
   plt.xticks(rotation=30, ha="right", fontsize=10) 
   plt.yticks(fontsize=10)
   plt.ylabel(Y_AXIS_LABEL, fontsize=12)
   plt.xlabel("Queue Type", fontsize=12)
   plt.grid(axis='y', linestyle=':', alpha=0.6)
   plt.tight_layout() 

   try:
      plt.savefig(OUTPUT_PLOT_FILE, dpi=150)
      print(f"\nPlot saved to {OUTPUT_PLOT_FILE}")
   except Exception as e:
      print(f"\nError saving plot: {e}")

if __name__ == '__main__':
   main()
