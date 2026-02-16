from pathlib import Path
import json
import logging

logger = logging.getLogger('bigquery_loader')


def load_manifest_to_bigquery(manifest_path: str, dataset: str = 'rvtools'):
    """Stub: load manifest/chunk data to BigQuery.

    Production: implement using google-cloud-bigquery client and schema mapping.
    For now, write a small record to a local JSON file as a placeholder.
    """
    p = Path(manifest_path)
    if not p.exists():
        raise FileNotFoundError(manifest_path)
    m = json.loads(p.read_text(encoding='utf-8'))
    out = p.with_suffix('.bqload.json')
    out.write_text(json.dumps({'dataset': dataset, 'manifest': m}, indent=2), encoding='utf-8')
    logger.info('Wrote BigQuery load placeholder to %s', out)
    return str(out)


if __name__ == '__main__':
    import sys
    if len(sys.argv) < 2:
        print('Usage: bigquery_loader.py <manifest_path>')
    else:
        print(load_manifest_to_bigquery(sys.argv[1]))
