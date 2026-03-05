import json
import logging
import os
from pathlib import Path

import pandas as pd
from neo4j import GraphDatabase

logger = logging.getLogger(__name__)

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASS = os.environ.get("NEO4J_PASSWORD", "")


def sync_manifest_to_neo4j(manifest_path: str):
    p = Path(manifest_path)
    if not p.exists():
        raise FileNotFoundError(manifest_path)
    m = json.loads(p.read_text(encoding="utf-8"))

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    with driver.session() as session:
        ingest_id = m.get("ingest_id", "ingest")
        session.run(
            "MERGE (i:Ingest {id:$id}) SET i.sheet=$sheet, i.chunk_count=$chunk_count",
            id=ingest_id,
            sheet=m.get('sheet'),
            chunk_count=m.get('chunk_count')
        )

        for ch in m.get('chunks', []):
            name = ch.get('name')
            rows = ch.get('rows')
            session.run(
                "MERGE (c:Chunk {name:$name}) SET c.rows=$rows",
                name=name, rows=rows
            )
            session.run(
                "MATCH (c:Chunk {name:$name}), (i:Ingest {id:$id}) MERGE (c)-[:PART_OF]->(i)",
                name=name, id=ingest_id
            )

            local = ch.get('local_path')
            if local:
                pch = Path(local)
                if pch.exists():
                    try:
                        df = pd.read_parquet(pch)
                        cols = {c.lower(): c for c in df.columns}
                        vm_col = cols.get('name') or cols.get('vmname')
                        host_col = cols.get('host') or cols.get('hostname')
                        for _, r in df.head(200).iterrows():
                            vm = str(r[vm_col]) if vm_col and not pd.isna(r[vm_col]) else None
                            host = str(r[host_col]) if host_col and not pd.isna(r[host_col]) else None
                            if vm:
                                session.run("MERGE (v:VM {name:$name})", name=vm)
                            if host:
                                session.run("MERGE (h:Host {name:$name})", name=host)
                            if vm and host:
                                session.run(
                                    "MATCH (v:VM {name:$v}), (h:Host {name:$h}) MERGE (h)-[:RUNS]->(v)",
                                    v=vm, h=host
                                )
                    except Exception:
                        logger.exception("Failed to sync chunk %s to Neo4j", pch)

    driver.close()


if __name__ == '__main__':
    import sys
    if len(sys.argv) < 2:
        print('Usage: neo4j_sync.py <manifest_path>')
    else:
        sync_manifest_to_neo4j(sys.argv[1])
