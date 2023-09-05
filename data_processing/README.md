# Data Processing of the modules

These scripts can be used to process the log files into plots. Here are the steps,
1. `python process_logfiles.py -g "/path/to/logs/YYYY/MM/*/*" -o /path/to/store/output -n <n-threads> -i ignore_modules.json -c categories.json`
   - this produces compressed CSV output files (1 per input file) where each row is 1 module that was loaded. The row includes these columns,
   ```python
   [
      "Module",
      "Version",
      "User",
      "Hostname",
      "Timestamp",
      "Python Executable",
      "Ignored",
      "Job ID",
      "Queue",
      "Job Size",
      "Account",
      "Node Number",
      "Job Name",
      "Job Directory",
      "PALS Depth",
      "PALS Rank ID",
      "PALS Local Rank ID",
      "PALS Node ID",
      "PMI Local Rank",
      "PMI Local Size",
      "PMI Rank",
      "PMI Size",
      "Categories"
   ]
   ```
2. `python parse_modfiles_to_jobfiles.py -g "/path/to/files/*"`
   - this produces compressed CSV output files (1 per input file) where each row is now a unique job id. The rows include these columns:
   ```python
   [
      'Categories',
      'Non-Ignored Modules',
      'Filesystems',
      'Award Category',
      'Walltime',
      'Nodes',
      'Runtime',
      'Exit Status',
      'User',
      'Hostname',
      'Queue',
      'Job Size',
      'Account',
      'Node Number',
      'Job Name',
      'Job Directory',
      'Timestamp',
      'PALS Depth',
      'PMI Size',
      'PMI Local Size'
   ]
   ```
3. `python plot_jobfiles.py -g "/path/to/files/*_byjob.csv.gz" -o <prefix-for-png-plots>`