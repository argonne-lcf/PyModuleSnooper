import pandas as pd
import json
import argparse
import glob
from multiprocessing import Pool
import os

def extract_data_from_log(log_filename, ignore_modules, categories):
   try:
     with open(log_filename, "r") as f:
        try:
          log_data = json.load(f)
        except:
          print("failed to parse the json in file: ",log_filename)
          return None
   except:
     print("failed to open file: ",log_filename)
     return None
   try:
      rows = []
      for module, version in log_data["versions"].items():
         env = log_data["env"]
         queue_name = "N/A"
         account = "N/A"
         nodenum = "N/A"
         job_name = "N/A"
         job_dir = "N/A"
         pals_depth = env.get("PALS_DEPTH","N/A")
         pals_local_rankid = env.get("PALS_LOCAL_RANKID","N/A")
         pals_nodeid = env.get("PALS_NODEID","N/A")
         pals_rankid = env.get("PALS_RANKID","N/A")

         if "PBS_JOBID" in env:
            job_id = env.get("PBS_JOBID", "N/A")  # Assuming the PBS_JOBID exists, else None is returned
            queue_name = env.get("PBS_QUEUE", "N/A")
            account = env.get("PBS_ACCOUNT","N/A")
            nodenum = env.get("PBS_NODENUM","N/A")
            job_name = env.get("PBS_JOBNAME","N/A")
            job_dir = env.get("PBS_JOBDIR","N/A")
         elif "COBALT_JOBID" in env:
            job_id = env.get("COBALT_JOBID","N/A")
            queue_name = env.get("COBALT_QUEUE", "N/A")
            job_size = env.get("COBALT_PARTSIZE", "N/A")
            account = env.get("COBALT_ACCOUNT","N/A")
            job_name = env.get("COBALT_JOBNAME","N/A")
         elif "login" in log_data["hostname"]:
            # ignore login node runs
            return None
         
         # PMI variables set by MPICH when running MPI
         job_size = env.get("PMI_SIZE",env.get("WORLD_SIZE",env.get("NRANKS",env.get("NUMRANKS",1))))
         job_id = env.get("PMI_JOBID","N/A")
         pmi_local_rank = env.get("PMI_LOCAL_RANK","N/A")
         pmi_local_size = env.get("PMI_LOCAL_SIZE","N/A")
         pmi_rank = env.get("PMI_RANK","N/A")
         pmi_size = env.get("PMI_SIZE","N/A")
         # print("job id: ",job_id)
         data_row = {
            "Module": module,
            "Version": version,
            "User": env.get("USER",env.get('PBS_O_LOGNAME',str(env.get('HOME')).split('/'[-1]))),
            "Hostname": log_data["hostname"],
            "Timestamp": log_data["timestamp"],
            "Python Executable": log_data["sys.executable"],
            "Ignored": (module in ignore_modules) or module.startswith('_') or ('.' in module),
            "Job ID": job_id,
            "Queue": queue_name,
            "Job Size": job_size,
            "Account": account,
            "Node Number": nodenum,
            "Job Name": job_name,
            "Job Directory": job_dir,
            "PALS Depth": pals_depth,
            "PALS Rank ID": pals_rankid,
            "PALS Local Rank ID": pals_local_rankid,
            "PALS Node ID": pals_nodeid,
            "PMI Local Rank": pmi_local_rank,
            "PMI Local Size": pmi_local_size,
            "PMI Rank": pmi_rank,
            "PMI Size": pmi_size,
         }

         data_row["Category"] = next((k for k, v in categories.items() if module in v), "none")
         
         rows.append(data_row)

      return pd.DataFrame(rows)
   except:
      print('failed to parse: ',log_filename)
      raise

def parallel_processing(log_files, ignore_modules, categories, n_processes):
   with Pool(n_processes) as p:
      dfs = p.starmap(extract_data_from_log, [(log, ignore_modules, categories) for log in log_files])
   valid_dfs = [df for df in dfs if df is not None]
   return pd.concat(valid_dfs, ignore_index=True)


if __name__ == "__main__":
   parser = argparse.ArgumentParser(description="""
Process JSON log files output by PyModuleSnooper to generate daily CSVs of module data.
Each module is converted to a row and useful information is added to each row like Job ID,
User name, hostname, etc. 

The script is currently written to process 1 month at a time.
""")
   parser.add_argument("-g", "--glob", help="Glob string to select log files for the month. Example: '/path/2023/07/??/*'", required=True)
   parser.add_argument("-o", "--output", help="Output directory for the compressed CSV files.", required=True)
   parser.add_argument("-n", "--nprocs", type=int, help="Number of parallel processes to use.", default=4)
   parser.add_argument("-i", "--ignore", help="JSON file with list of modules to ignore. Example Contents: ['os','sys',...]", required=True)
   parser.add_argument("-c", "--category", help="JSON file defining module categories. Example Contents: {'AI':['tensorflow',..],'IO':['pandas','hdf5'],..}", required=True)
   
   parser.add_argument("--overwrite",action="store_true",help="overwrite existing output files.",default=False)

   args = parser.parse_args()

   # Load modules to ignore
   with open(args.ignore, "r") as f:
      ignore_modules = json.load(f)

   # Load module categories
   with open(args.category, "r") as f:
      categories = json.load(f)


   # Split path into segments
   path_segments = args.glob.split('/')
   year, month = path_segments[-4], path_segments[-3]

   base_path = args.glob.rsplit('/', 2)[0]  # Extract up to "/2023/07"
   days = sorted(set([os.path.basename(p) for p in glob.glob(base_path + '/??')]))

   for day in days:
      daily_glob = base_path + f'/{day}/*'
      daily_log_files = glob.glob(daily_glob)

      if not daily_log_files:
         print(f"No log files found for {day}. Skipping.")
         continue

      print(f"Processing {len(daily_log_files)} files for {day}...",end='')
      daily_output_path = os.path.join(args.output, f'modules_{year}_{month}_{day}.csv.gz')
      if os.path.exists(daily_output_path) and not args.overwrite:
         print(" skipped.")
         continue

      daily_df = parallel_processing(daily_log_files, ignore_modules, categories, args.nprocs)
      daily_df.to_csv(daily_output_path, index=False, compression='gzip')
      print(" done processing.")
