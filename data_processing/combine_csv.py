import pandas as pd
import argparse
import glob

if __name__ == "__main__":
   parser = argparse.ArgumentParser(description="Combine multiple compressed CSV files into one.")
   parser.add_argument("-g","--input_glob", help="Glob string to select input compressed CSV files.")
   parser.add_argument("-o","--output_file", help="Name of the output combined compressed CSV file.")
   args = parser.parse_args()

   # Collect CSV files
   all_files = glob.glob(args.input_glob)
   print(f"Found {len(all_files)} files to combine.")

   dfs = []
   for idx, file in enumerate(all_files, 1):
      dfs.append(pd.read_csv(file, compression='gzip'))
      print(f"Processed file {idx}/{len(all_files)}: {file}")

   combined_df = pd.concat(dfs, ignore_index=True)

   # Save to compressed CSV
   combined_df.to_csv(args.output_file, index=False, compression='gzip')
   print(f"Combined CSV saved to {args.output_file}.")
