"""Canonical schema aliases for RVTools sheets.

Maps logical column names to common aliases found in RVTools exports.
Used to reliably locate columns across variants.
"""

SCHEMA = {
    'vInfo': {
        'name': ['name', 'vmname', 'displayname', 'vm name'],
        'host': ['host', 'hostname', 'esxi host', 'esxi'],
        'numcpu': ['numcpu', 'vcpu', 'num vcpu', 'cpu'],
        'memorymb': ['memorymb', 'memory', 'mem', 'memory mb'],
        'vmtools': ['vmwaretools', 'vmtools', 'tools', 'vm tools'],
        'datastore': ['datastore', 'datastorename', 'ds', 'storage'],
        'network': ['network', 'portgroup', 'nicnetwork', 'network name']
    },
    'vHost': {
        'name': ['name', 'hostname', 'host name'],
        'version': ['version', 'productversion', 'esxi version'],
        'cpu': ['cpu', 'numcpu', 'num cpu', 'cores'],
        'memorymb': ['memorymb', 'memory', 'mem']
    }
}

def find_column(df_columns, sheet_name: str, logical_name: str):
    cols = [c for c in df_columns]
    lower_map = {c.lower(): c for c in cols}

    schema = SCHEMA.get(sheet_name, {})
    aliases = schema.get(logical_name, [])
    for a in aliases:
        if a in lower_map:
            return lower_map[a]
    # fallback: try substring match
    for c_l, c_orig in lower_map.items():
        for a in aliases:
            if a in c_l:
                return c_orig
    return None
