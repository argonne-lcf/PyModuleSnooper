import pandas as pd
import argparse
import glob
import matplotlib.pyplot as plt

def plot_categories(df, filename, accounts_to_exclude=None, users_to_exclude=None):
   # Filter out the DataFrame based on provided account names and user names to exclude
   if accounts_to_exclude:
      df = df[~df['Account'].isin(accounts_to_exclude)]
   if users_to_exclude:
      df = df[~df['User'].isin(users_to_exclude)]

   # We'll use explode to handle the lists in the "Categories" column
   cat_node_hours = df.explode('Categories').groupby('Categories')['Node-Hours'].sum()
   plt.figure(figsize=(10, 6))
   cat_node_hours.plot.pie(autopct='%1.1f%%', startangle=90)
   plt.title(f'Job Module Categories - Total Node-Hours: {cat_node_hours.sum():.2f}')
   plt.ylabel('')
   plt.tight_layout()
   plt.savefig(filename)
   plt.close()


def plot_timeline_by_category(df, filename, accounts_to_exclude=None, users_to_exclude=None):
   # Filter out the DataFrame based on provided account names and user names to exclude
   if accounts_to_exclude:
      df = df[~df['Account'].isin(accounts_to_exclude)]
   if users_to_exclude:
      df = df[~df['User'].isin(users_to_exclude)]

   # Set the Timestamp column as the index, creating a new DataFrame
   temp_df = df.set_index('Timestamp')
   
   # Resample the dataframe based on a day ('D'). For each bin, explode the Categories 
   # column and count the unique occurrences for each category.
   timeline = temp_df.resample('D').apply(lambda x: x.explode('Categories')['Categories'].value_counts()).unstack().fillna(0).cumsum(axis=1)
   
   # Plot the binned timeline using an area plot
   timeline.plot(kind='area', stacked=True, figsize=(12, 6))
   plt.title('Number of Jobs Over Time (by Categories)')
   plt.ylabel('Number of Jobs')
   plt.xlabel('Time')
   plt.tight_layout()
   plt.savefig(filename)
   plt.close()

def plot_timeline_by_user(df, filename, accounts_to_exclude=None, users_to_exclude=None):
   # Filter out the DataFrame based on provided account names and user names to exclude
   if accounts_to_exclude:
      df = df[~df['Account'].isin(accounts_to_exclude)]
   if users_to_exclude:
      df = df[~df['User'].isin(users_to_exclude)]

   # Set the Timestamp column as the index, creating a new DataFrame
   temp_df = df.set_index('Timestamp')
   
   # Resample the dataframe based on a day ('D'). For each bin, count the unique occurrences for each user.
   timeline = temp_df.resample('D').apply(lambda x: x['User'].value_counts()).unstack().fillna(0).cumsum(axis=1)
   
   # Plot the binned timeline using an area plot
   timeline.plot(kind='area', stacked=True, figsize=(12, 6))
   plt.title('Number of Jobs Over Time (by User)')
   plt.ylabel('Number of Jobs')
   plt.xlabel('Time')
   plt.tight_layout()
   plt.savefig(filename)
   plt.close()

def plot_timeline_by_account(df, filename, accounts_to_exclude=None, users_to_exclude=None):
   # Filter out the DataFrame based on provided account names and user names to exclude
   if accounts_to_exclude:
      df = df[~df['Account'].isin(accounts_to_exclude)]
   if users_to_exclude:
      df = df[~df['User'].isin(users_to_exclude)]

   # Set the Timestamp column as the index, creating a new DataFrame
   temp_df = df.set_index('Timestamp')
   
   # Resample the dataframe based on a day ('D'). For each bin, count the unique occurrences for each account.
   timeline = temp_df.resample('D').apply(lambda x: x['Account'].value_counts()).unstack().fillna(0).cumsum(axis=1)
   
   # Plot the binned timeline using an area plot
   timeline.plot(kind='area', stacked=True, figsize=(12, 6))
   plt.title('Number of Jobs Over Time (by Account)')
   plt.ylabel('Number of Jobs')
   plt.xlabel('Time')
   plt.tight_layout()
   plt.savefig(filename)
   plt.close()


def plot_job_sizes(df, filename, accounts_to_exclude=None, users_to_exclude=None):
   # Filter out the DataFrame based on provided account names and user names to exclude
   if accounts_to_exclude:
      df = df[~df['Account'].isin(accounts_to_exclude)]
   if users_to_exclude:
      df = df[~df['User'].isin(users_to_exclude)]

   bins = [0, 10, 24, 99, max(128,df['Job Size'].max() + 1)]
   labels = ['1-10 nodes', '11-24 nodes', '25-99 nodes', '>=100 nodes']
   df['Job Size Bin'] = pd.cut(df['Job Size'], bins=bins, labels=labels, right=False)
   size_counts = df['Job Size Bin'].value_counts().sort_index()
   plt.figure(figsize=(10, 6))
   size_counts.plot.pie(autopct='%1.1f%%', startangle=90)
   plt.title(f'Job Sizes - Total Jobs: {df.shape[0]}')
   plt.ylabel('')
   plt.tight_layout()
   plt.savefig(filename)
   plt.close()

def plot_accounts(df, filename, accounts_to_exclude=None, users_to_exclude=None):
   # Filter out the DataFrame based on provided account names and user names to exclude
   if accounts_to_exclude:
      df = df[~df['Account'].isin(accounts_to_exclude)]
   if users_to_exclude:
      df = df[~df['User'].isin(users_to_exclude)]

   account_node_hours = df.groupby('Account')['Node-Hours'].sum()
   account_node_hours.plot(kind='bar', figsize=(12, 6))
   plt.title('Node-Hours by Account')
   plt.ylabel('Node-Hours')
   plt.xlabel('Account')
   plt.yscale("log")
   plt.tight_layout()
   plt.savefig(filename)
   plt.close()

def plot_users(df, filename, accounts_to_exclude=None, users_to_exclude=None):
   # Filter out the DataFrame based on provided account names and user names to exclude
   if accounts_to_exclude:
      df = df[~df['Account'].isin(accounts_to_exclude)]
   if users_to_exclude:
      df = df[~df['User'].isin(users_to_exclude)]

   user_node_hours = df.groupby('User')['Node-Hours'].sum()
   user_node_hours.head(20).plot(kind='bar', figsize=(12, 6))  # Display top 20 users for clarity
   plt.title('Node-Hours by User')
   plt.ylabel('Node-Hours')
   plt.xlabel('User')
   plt.yscale("log")
   plt.tight_layout()
   plt.savefig(filename)
   plt.close()

def plot_award_category(df, filename, accounts_to_exclude=None, users_to_exclude=None):
   if accounts_to_exclude:
      df = df[~df['Account'].isin(accounts_to_exclude)]
   if users_to_exclude:
      df = df[~df['User'].isin(users_to_exclude)]
      
   award_node_hours = df.groupby('Award Category')['Node-Hours'].sum()
   plt.figure(figsize=(10, 6))
   award_node_hours.plot.pie(autopct='%1.1f%%', startangle=90)
   plt.title(f'Node-Hours by Award Category - Total Node-Hours: {award_node_hours.sum():.2f}')
   plt.ylabel('')
   plt.tight_layout()
   plt.savefig(filename)
   plt.close()

def plot_filesystems(df, filename, accounts_to_exclude=None, users_to_exclude=None):
   if accounts_to_exclude:
      df = df[~df['Account'].isin(accounts_to_exclude)]
   if users_to_exclude:
      df = df[~df['User'].isin(users_to_exclude)]

   # Split the 'Filesystems' string into a list
   df['Filesystems'] = df['Filesystems'].str.split(':')

   # Create a new dataframe to explode and calculate node-hours for each filesystem
   exploded_df = df.explode('Filesystems')
   filesystem_node_hours = exploded_df.groupby('Filesystems')['Node-Hours'].sum()
   
   plt.figure(figsize=(10, 6))
   filesystem_node_hours.plot.pie(autopct='%1.1f%%', startangle=90)
   plt.title(f'Node-Hours by Filesystem - Total Node-Hours: {filesystem_node_hours.sum():.2f}')
   plt.ylabel('')
   plt.tight_layout()
   plt.savefig(filename)
   plt.close()

def plot_runtime_to_walltime_ratio(df, filename, accounts_to_exclude=None, users_to_exclude=None):
   if accounts_to_exclude:
      df = df[~df['Account'].isin(accounts_to_exclude)]
   if users_to_exclude:
      df = df[~df['User'].isin(users_to_exclude)]
   
   # Calculate the ratio of runtime to wall-time
   df['Runtime-Walltime Ratio'] = df['Runtime'] / df['Walltime']

   # Define bins for the ratio
   bins = [0, 0.1, 0.2, 0.4, 0.6, 0.8, 0.9, float('inf')]
   labels = ['0-10%', '10-20%', '20-40%', '40-60%', '60-80%', '80-90%', '90-100%']
   df['Ratio Bin'] = pd.cut(df['Runtime-Walltime Ratio'], bins=bins, labels=labels, right=False)

   # Plot histogram
   plt.figure(figsize=(10, 6))
   df['Ratio Bin'].value_counts(sort=False).plot(kind='bar', color='blue')
   plt.title('Distribution of Runtime to Walltime Ratio')
   plt.ylabel('Number of Jobs')
   plt.xlabel('Runtime to Walltime Ratio')
   plt.tight_layout()
   plt.savefig(filename)
   plt.close()

def plot_runtime_to_walltime_ratio_weighted(df, filename, accounts_to_exclude=None, users_to_exclude=None):
   if accounts_to_exclude:
      df = df[~df['Account'].isin(accounts_to_exclude)]
   if users_to_exclude:
      df = df[~df['User'].isin(users_to_exclude)]
   
   # Calculate the ratio of runtime to wall-time
   df['Runtime-Walltime Ratio'] = df['Runtime'] / df['Walltime']

   # Define bins for the ratio
   bins = [0, 0.1, 0.2, 0.4, 0.6, 0.8, 0.9, float('inf')]
   labels = ['0-10%', '10-20%', '20-40%', '40-60%', '60-80%', '80-90%', '90-100%']

   # Create a new column for bins
   df['Ratio Bin'] = pd.cut(df['Runtime-Walltime Ratio'], bins=bins, labels=labels, right=False)

   # Group by the bin and sum up the node-hours for each group
   node_hours_per_bin = df.groupby('Ratio Bin')['Node-Hours'].sum()

   # Plot histogram
   plt.figure(figsize=(10, 6))
   node_hours_per_bin.plot(kind='bar', color='blue')
   plt.title('Distribution of Runtime to Walltime Ratio (Weighted by Node-Hours)')
   plt.ylabel('Total Node-Hours')
   plt.xlabel('Runtime to Walltime Ratio')
   plt.tight_layout()
   plt.savefig(filename)
   plt.close()

def plot_module_usage_node_hours(df, filename, accounts_to_exclude=None, users_to_exclude=None):
   if accounts_to_exclude:
      df = df[~df['Account'].isin(accounts_to_exclude)]
   if users_to_exclude:
      df = df[~df['User'].isin(users_to_exclude)]

   # Calculate node-hours for each job
   df['Node-Hours'] = df['Nodes'] * df['Runtime']

   # Filter the DataFrame for jobs that used either tensorflow or torch
   tensorflow_jobs = df[df['Non-Ignored Modules'].apply(lambda x: 'tensorflow' in x)]
   torch_jobs = df[df['Non-Ignored Modules'].apply(lambda x: 'torch' in x)]

   # Calculate node-hours for each module
   tensorflow_node_hours = tensorflow_jobs['Node-Hours'].sum()
   torch_node_hours = torch_jobs['Node-Hours'].sum()

   # Create data for the pie plot
   labels = ['TensorFlow', 'Torch']
   sizes = [tensorflow_node_hours, torch_node_hours]

   # Plot
   plt.figure(figsize=(10, 6))
   plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
   plt.title('Node-Hours by Module Usage')
   plt.tight_layout()
   plt.savefig(filename)
   plt.close()



if __name__ == "__main__":
   parser = argparse.ArgumentParser(description="Generate plots from processed job data.")
   parser.add_argument("-g", "--input_glob", help="Glob pattern to select the input CSV files. Can be multiple",action="append",required=True)
   parser.add_argument("-o", "--output_prefix", help="Output prefix for generated plot files.",required=True)
   args = parser.parse_args()

   all_data = []
   print(args.input_glob)
   for glob_str in args.input_glob:
      filelist = glob.glob(glob_str)
      for file in filelist:
         print(f"Reading data from {file}...")
         all_data.append(pd.read_csv(file, compression='gzip', converters={'Categories': eval}, parse_dates=['Timestamp']))
   
   df = pd.concat(all_data, ignore_index=True)
   print(f"Combined data from {len(all_data)} files into one DataFrame.")

   # calculate node-hours
   # print(df['Nodes'], df['Runtime'])
   df['Node-Hours'] = df['Nodes'] * df['Runtime']

   df = df[df['Node-Hours'] > 0]

   accounts_to_exclude = None
   accounts_to_exclude = ['datascience']
   plot_categories(df, f"{args.output_prefix}_categories.png",accounts_to_exclude=accounts_to_exclude)
   plot_timeline_by_category(df, f"{args.output_prefix}_timeline_cat.png",accounts_to_exclude=accounts_to_exclude)
   plot_timeline_by_user(df, f"{args.output_prefix}_timeline_user.png",accounts_to_exclude=accounts_to_exclude)
   plot_timeline_by_account(df, f"{args.output_prefix}_timeline_accounts.png",accounts_to_exclude=accounts_to_exclude)
   plot_job_sizes(df, f"{args.output_prefix}_jobsizes.png",accounts_to_exclude=accounts_to_exclude)
   plot_accounts(df, f"{args.output_prefix}_accounts.png",accounts_to_exclude=accounts_to_exclude)
   plot_users(df, f"{args.output_prefix}_users.png",accounts_to_exclude=accounts_to_exclude)
   plot_award_category(df,f"{args.output_prefix}_award_category.png",accounts_to_exclude=accounts_to_exclude)
   plot_filesystems(df, f"{args.output_prefix}_filesystems.png", accounts_to_exclude=accounts_to_exclude)
   plot_runtime_to_walltime_ratio(df, f"{args.output_prefix}_runtime_walltime_ratio.png", accounts_to_exclude=accounts_to_exclude)
   plot_runtime_to_walltime_ratio_weighted(df, f"{args.output_prefix}_runtime_walltime_ratio_weighted.png", accounts_to_exclude=accounts_to_exclude)
   plot_module_usage_node_hours(df, f"{args.output_prefix}_module_usage_node_hours.png", accounts_to_exclude=accounts_to_exclude)


   
   print("All plots generated successfully.")
