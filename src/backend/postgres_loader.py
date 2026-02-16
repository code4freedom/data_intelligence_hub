from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Text
from pathlib import Path
import pandas as pd
import os


def get_engine():
    # default to docker-compose postgres service
    user = os.environ.get('POSTGRES_USER', 'rvtools')
    pwd = os.environ.get('POSTGRES_PASSWORD', 'rvtools')
    host = os.environ.get('POSTGRES_HOST', 'postgres')
    db = os.environ.get('POSTGRES_DB', 'rvtools')
    port = os.environ.get('POSTGRES_PORT', '5432')
    url = f'postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}'
    return create_engine(url)


def ensure_tables(engine):
    meta = MetaData()
    vms = Table('vms', meta,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('name', String(255)),
                Column('host', String(255)),
                Column('numcpu', Integer),
                Column('memorymb', Integer),
                Column('vmtools', String(255)))
    hosts = Table('hosts', meta,
                  Column('id', Integer, primary_key=True, autoincrement=True),
                  Column('name', String(255)),
                  Column('version', String(64)),
                  Column('cpu', Integer),
                  Column('memorymb', Integer))
    meta.create_all(engine)
    return vms, hosts


def load_manifest_into_postgres(manifest_path: str):
    p = Path(manifest_path)
    if not p.exists():
        raise FileNotFoundError(manifest_path)
    e = get_engine()
    vms_tbl, hosts_tbl = ensure_tables(e)

    # simple ingestion: for each chunk, read parquet and insert VM rows if sheet is vInfo
    m = pd.read_json(str(p))
    sheet = m.get('sheet')
    for ch in m.get('chunks', []):
        local = ch.get('local_path')
        if not local:
            continue
        pch = Path(local)
        if not pch.exists():
            continue
        df = pd.read_parquet(pch)
        # heuristics for vInfo rows
        if sheet.lower().startswith('vinfo') or 'vm' in sheet.lower():
            # normalize columns lower
            cols = {c.lower(): c for c in df.columns}
            name_col = cols.get('name') or cols.get('vmname')
            host_col = cols.get('host') or cols.get('hostname')
            cpu_col = cols.get('numcpu') or cols.get('vcpu')
            mem_col = cols.get('memorymb') or cols.get('memory')
            tools_col = cols.get('vmtools') or cols.get('tools')
            records = []
            for _, r in df.iterrows():
                records.append({
                    'name': str(r[name_col]) if name_col in r and not pd.isna(r[name_col]) else None,
                    'host': str(r[host_col]) if host_col in r and not pd.isna(r[host_col]) else None,
                    'numcpu': int(r[cpu_col]) if cpu_col in r and not pd.isna(r[cpu_col]) else None,
                    'memorymb': int(r[mem_col]) if mem_col in r and not pd.isna(r[mem_col]) else None,
                    'vmtools': str(r[tools_col]) if tools_col in r and not pd.isna(r[tools_col]) else None,
                })
            if records:
                pd.DataFrame(records).to_sql('vms', e, if_exists='append', index=False)
        elif 'host' in sheet.lower():
            cols = {c.lower(): c for c in df.columns}
            name_col = cols.get('name') or cols.get('hostname')
            ver_col = cols.get('version') or cols.get('product')
            cpu_col = cols.get('cpu') or cols.get('numcpu')
            mem_col = cols.get('memorymb') or cols.get('memory')
            records = []
            for _, r in df.iterrows():
                records.append({
                    'name': str(r[name_col]) if name_col in r and not pd.isna(r[name_col]) else None,
                    'version': str(r[ver_col]) if ver_col in r and not pd.isna(r[ver_col]) else None,
                    'cpu': int(r[cpu_col]) if cpu_col in r and not pd.isna(r[cpu_col]) else None,
                    'memorymb': int(r[mem_col]) if mem_col in r and not pd.isna(r[mem_col]) else None,
                })
            if records:
                pd.DataFrame(records).to_sql('hosts', e, if_exists='append', index=False)

    return True


if __name__ == '__main__':
    import sys
    if len(sys.argv) < 2:
        print('Usage: postgres_loader.py <manifest_path>')
    else:
        print(load_manifest_into_postgres(sys.argv[1]))
