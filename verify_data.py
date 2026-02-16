import pandas as pd
from pathlib import Path

df = pd.read_parquet('data/chunks/chunk_vInfo_000000.parquet')
print(f"\n{'='*70}")
print(f"RVTools vInfo Data Summary - SizingWorkshop Dataset")
print(f"{'='*70}")
print(f"Total Rows (VMs):           {len(df)}")

print(f"\n{'='*70}")
print(f"Key Metrics for PDF Report:")
print(f"{'='*70}")

# Get unique hosts
if 'Host' in df.columns:
    hosts = df['Host'].nunique()
    print(f"Total Hosts:                {hosts}")
else:
    print("⚠️  'Host' column not found")

# CPU sum
if 'CPUs' in df.columns:
    total_cpu = df['CPUs'].sum()
    print(f"Total CPU Cores:            {int(total_cpu)}")
else:
    print("⚠️  'CPUs' column not found")

# Memory 
if 'Memory' in df.columns:
    total_mem_mb = df['Memory'].sum()
    total_mem_gb = total_mem_mb / 1024
    total_mem_tb = total_mem_gb / 1024
    print(f"Total Memory:               {total_mem_mb:,.0f} MB")
    print(f"                    or      {total_mem_gb:,.1f} GB")
    print(f"                    or      {total_mem_tb:.2f} TB")
else:
    print("⚠️  'Memory' column not found")

# Power state
if 'Powerstate' in df.columns:
    print(f"\nVM Power States:")
    pwr = df['Powerstate'].value_counts()
    for state, count in pwr.items():
        print(f"  {state}: {count}")

# EOS Risk (look for version indicators)
if 'OS according to the configuration file' in df.columns:
    os_col = df['OS according to the configuration file']
    print(f"\nOS Versions (config):")
    print(os_col.value_counts().head(10))

print(f"\n{'='*70}")
print(f"Column Names:")
for col in df.columns:
    print(f"  - {col}")
print(f"{'='*70}\n")
