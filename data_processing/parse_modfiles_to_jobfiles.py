import pandas as pd
import argparse
import glob
import os
import subprocess
import json

def get_seconds(time_str):
   if time_str:
      parts = time_str.split(":")
   else:
      return 0
   if len(parts) == 3:
      return int(parts[0])*60*60 + int(parts[1])*60 + int(parts[0])
   else:
      return 0

def get_job_info_as_json(job_id):
   try:
      # Run the qstat command with the given job_id and capture the output
      result = subprocess.run(
         ["/opt/pbs/bin/qstat", "-fx", "-F", "json", str(job_id)],
         capture_output=True, text=True, check=True
      )

      # Parse the output as JSON
      json_output = json.loads(result.stdout)
      return json_output
   except subprocess.CalledProcessError as e:
      print(f"Error running qstat for job {job_id}: {e}")
      return None


# def process_dataframe_old(df):
#    # A helper function to aggregate module categories and distinct non-ignored modules
#    def agg_func(x):
#       # Find unique categories excluding 'none'
#       categories = [cat for cat in x['Category'].unique().tolist() if cat != 'none']
      
#       # If no other categories were found, add 'none'
#       if not categories:
#          categories = ['none']
      
#       # Find distinct non-ignored modules
#       non_ignored_modules = x[x['Ignored'] == False]['Module'].unique().tolist()
      
#       # Return a series with the aggregated results
#       return pd.Series([categories, non_ignored_modules], index=['Categories', 'Non-Ignored Modules'])

#    # Group by Job ID and apply our aggregation function
#    result = df.groupby('Job ID').apply(agg_func)

#    # Add other job-related columns
#    job_cols = ['User', 'Hostname', 'Queue', 'Job Size', 'Account', 'Node Number', 'Job Name', 'Job Directory', 
#                'Timestamp', 'PALS Depth', 'PMI Size', 'PMI Local Size']
#    for col in job_cols:
#       result[col] = df.groupby('Job ID')[col].first()
   
#    result = result.reset_index()

#    return result

def process_dataframe(df):
   def agg_func(x):
      categories = [cat for cat in x['Category'].unique().tolist() if cat != 'none']
      
      if not categories:
         categories = ['none']
      
      non_ignored_modules = x[x['Ignored'] == False]['Module'].unique().tolist()
      
      job_data = get_job_info_as_json(x['Job ID'].iloc[0])
      if job_data:
         key = list(job_data["Jobs"].keys())[0]
         job_details = job_data["Jobs"][key]
         
         # Extract required data from Resource_List and resources_used
         filesystems = job_details.get("Resource_List", {}).get("filesystems", None)
         award_category = job_details.get("Resource_List", {}).get("award_category", None)
         walltime_resource = job_details.get("Resource_List", {}).get("walltime", None)
         select = job_details.get("Resource_List", {}).get("select", None)
         if(isinstance(select,str)):
            select = int(select.split(":")[0])
         
         walltime_used = job_details.get("resources_used", {}).get("walltime", None)
         exit_status = job_details.get("Exit_status", None)
         
         return pd.Series([categories, non_ignored_modules, filesystems, award_category, walltime_resource, select, walltime_used, exit_status], 
                           index=['Categories', 'Non-Ignored Modules', 'Filesystems', 'Award Category', 'Walltime', 'Nodes', 'Runtime', 'Exit Status'])
      else:
         return pd.Series([categories, non_ignored_modules, None, None, None, None, None, None], 
                           index=['Categories', 'Non-Ignored Modules', 'Filesystems', 'Award Category', 'Walltime', 'Nodes', 'Runtime', 'Exit Status'])

   result = df.groupby('Job ID').apply(agg_func)

   job_cols = ['User', 'Hostname', 'Queue', 'Job Size', 'Account', 'Node Number', 'Job Name', 'Job Directory', 
               'Timestamp', 'PALS Depth', 'PMI Size', 'PMI Local Size']
   for col in job_cols:
      result[col] = df.groupby('Job ID')[col].first()
   
   result['Walltime'] = result['Walltime'].apply(get_seconds)
   result['Runtime'] = result['Runtime'].apply(get_seconds)

   return result.reset_index()

if __name__ == "__main__":
   parser = argparse.ArgumentParser(description="Aggregate DataFrame based on unique Job ID.")
   parser.add_argument("-g", "--input_glob", help="Glob pattern to select the input compressed CSV files.")
   parser.add_argument("-p", "--postfix", default="_byjob", help="Postfix to append to the output filenames.")

   parser.add_argument("--overwrite",action="store_true",help="overwrite existing output files.",default=False)

   args = parser.parse_args()

   # Iterate over the files matched by the glob
   for file in sorted(glob.glob(args.input_glob)):
      output_filename = file.replace('.csv.gz', args.postfix + '.csv.gz')
      if os.path.exists(output_filename) and not args.overwrite:
         print(f"File exists: {output_filename}")
         continue
      
      # Read the compressed CSV file
      df = pd.read_csv(file, compression='gzip')
      processed_df = process_dataframe(df)

      # Create the output filename by replacing the existing ".csv.gz" with the postfix + ".csv.gz"
      processed_df.to_csv(output_filename, index=False, compression='gzip')
      print(f"Processed data from {file} saved to {output_filename}.")
