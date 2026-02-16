import pandas as pd
import numpy as np
from pathlib import Path
import json


def generate_vm_parquet(out_dir: str, sheet: str = 'vInfo', num_vms: int = 10000, chunk_size: int = 5000, ingest_id: str = 'synthetic'):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    rows = []
    for i in range(num_vms):
        rows.append({
            'Name': f'vm-{i:06d}',
            'Host': f'host-{(i%200):04d}',
            'NumCPU': np.random.choice([1,2,4,8]),
            'MemoryMB': np.random.choice([2048,4096,8192,16384]),
            'VMTools': np.random.choice(['tools-ok', 'tools-old', 'tools-none'], p=[0.8,0.15,0.05])
        })
    df = pd.DataFrame(rows)
    # chunk into parquet files
    written = []
    for i in range(0, len(df), chunk_size):
        part = df[i:i+chunk_size]
        p = out / f'chunk_{sheet}_{i//chunk_size:06d}.parquet'
        part.to_parquet(p, index=False)
        written.append({'name': p.name, 'rows': len(part), 'local_path': str(p)})
    manifest = {'ingest_id': ingest_id, 'sheet': sheet, 'chunk_count': len(written), 'total_rows': num_vms, 'chunks': written}
    mpath = out / f'manifest_{sheet}_{ingest_id}.json'
    mpath.write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    print('Wrote', len(written), 'chunks to', out)
    print('Manifest:', mpath)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--out', default='./data/chunks')
    parser.add_argument('--vms', type=int, default=10000)
    parser.add_argument('--chunk-size', type=int, default=5000)
    args = parser.parse_args()
    generate_vm_parquet(args.out, num_vms=args.vms, chunk_size=args.chunk_size)
