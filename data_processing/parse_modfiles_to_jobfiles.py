import pandas as pd
import argparse
import glob
import os

def process_dataframe(df):
    # A helper function to aggregate module categories and distinct non-ignored modules
    def agg_func(x):
        # Find unique categories
        categories = x['Category'].unique().tolist()
        # Find distinct non-ignored modules
        non_ignored_modules = x[x['Ignored'] == False]['Module'].unique().tolist()
        # Return a series with the aggregated results
        return pd.Series([categories, non_ignored_modules], index=['Categories', 'Non-Ignored Modules'])

    # Group by Job ID and apply our aggregation function
    result = df.groupby('Job ID').apply(agg_func)

    # Add other job-related columns
    job_cols = ['User', 'Hostname', 'Queue', 'Job Size', 'Account', 'Node Number', 'Job Name', 'Job Directory', 
                'Timestamp', 'PALS Depth', 'PMI Size', 'PMI Local Size']
    for col in job_cols:
        result[col] = df.groupby('Job ID')[col].first()
    
    return result.reset_index()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Aggregate DataFrame based on unique Job ID.")
    parser.add_argument("-i", "--input_glob", help="Glob pattern to select the input compressed CSV files.")
    parser.add_argument("-p", "--postfix", default="_byjob", help="Postfix to append to the output filenames.")
    args = parser.parse_args()

    # Iterate over the files matched by the glob
    for file in glob.glob(args.input_glob):
        # Read the compressed CSV file
        df = pd.read_csv(file, compression='gzip')
        processed_df = process_dataframe(df)

        # Create the output filename by replacing the existing ".csv.gz" with the postfix + ".csv.gz"
        output_filename = file.replace('.csv.gz', args.postfix + '.csv.gz')
        processed_df.to_csv(output_filename, index=False, compression='gzip')
        print(f"Processed data from {file} saved to {output_filename}.")

