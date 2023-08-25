import pandas as pd
import argparse
import glob
import matplotlib.pyplot as plt

def plot_categories(df, filename):
    # We'll use explode to handle the lists in the "Categories" column
    cat_counts = df.explode('Categories')['Categories'].value_counts()
    plt.figure(figsize=(10, 6))
    cat_counts.plot.pie(autopct='%1.1f%%', startangle=90)
    plt.title(f'Job Module Categories - Total Jobs: {df.shape[0]}')
    plt.ylabel('')
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()

def plot_timeline(df, filename):
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    df.set_index('Timestamp', inplace=True)
    timeline = df.resample('D').apply(lambda x: x.explode('Categories')['Categories'].value_counts())
    timeline.plot(kind='area', stacked=True, figsize=(12, 6))
    plt.title('Number of Jobs Over Time')
    plt.ylabel('Number of Jobs')
    plt.xlabel('Time')
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()

def plot_job_sizes(df, filename):
    bins = [0, 8, 64, 128, 256, df['Job Size'].max() + 1]
    labels = ['1-8 nodes', '9-64 nodes', '65-128 nodes', '129-256 nodes', '>=257 nodes']
    df['Job Size Bin'] = pd.cut(df['Job Size'], bins=bins, labels=labels, right=False)
    size_counts = df['Job Size Bin'].value_counts().sort_index()
    plt.figure(figsize=(10, 6))
    size_counts.plot.pie(autopct='%1.1f%%', startangle=90)
    plt.title(f'Job Sizes - Total Jobs: {df.shape[0]}')
    plt.ylabel('')
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()

def plot_accounts(df, filename):
    account_counts = df['Account'].value_counts()
    account_counts.plot(kind='bar', figsize=(12, 6))
    plt.title('Number of Jobs by Account')
    plt.ylabel('Number of Jobs')
    plt.xlabel('Account')
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()

def plot_users(df, filename):
    user_counts = df['User'].value_counts()
    user_counts.head(20).plot(kind='bar', figsize=(12, 6))  # Display top 20 users for clarity
    plt.title('Number of Jobs by User')
    plt.ylabel('Number of Jobs')
    plt.xlabel('User')
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate plots from processed job data.")
    parser.add_argument("-i", "--input_glob", help="Glob pattern to select the input CSV files.")
    parser.add_argument("-o", "--output_prefix", help="Output prefix for generated plot files.")
    args = parser.parse_args()

    all_data = []
    for file in glob.glob(args.input_glob):
        print(f"Reading data from {file}...")
        all_data.append(pd.read_csv(file, compression='gzip', converters={'Categories': eval}, parse_dates=['Timestamp']))
    
    df = pd.concat(all_data, ignore_index=True)
    print(f"Combined data from {len(all_data)} files into one DataFrame.")

    plot_categories(df, f"{args.output_prefix}_categories.png")
    plot_timeline(df, f"{args.output_prefix}_timeline.png")
    plot_job_sizes(df, f"{args.output_prefix}_jobsizes.png")
    plot_accounts(df, f"{args.output_prefix}_accounts.png")
    plot_users(df, f"{args.output_prefix}_users.png")
        
    print("All plots generated successfully.")
