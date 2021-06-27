from dataclasses import field
from google.cloud import bigquery
import json
import awkward as ak
from numpy import isin
import click

types_to_bq = {
    'int64': 'INTEGER',
    'int32': 'INTEGER',
    'int16': 'INTEGER',
    'uint16': 'INTEGER',
    'uint64': 'INTEGER',
    'float64': 'FLOAT',
    'int8': 'INTEGER',
    'bool': 'BOOLEAN',
    'uint8': 'INTEGER',
    'float32': 'FLOAT',
    'string': 'STRING'
}

def walk_type(top,indent = ''):
    indent += '>'
    if isinstance(top,ak.types.RecordType):
        field_types = []
        for k,v in top.fielditems():
            if k == 'TrigMatchedObjects':
                continue
            field_type = walk_type(v,indent)
            field_type['name'] = k
            if field_type['type'] != 'unknown':
                field_types.append(field_type)
                
            
        return {'type': 'RECORD', 'fields': field_types, 'aux': 'composed', 'mode': 'REQUIRED'}
    elif isinstance(top,ak.types.ListType):
        if top.typestr in types_to_bq:
            element_type = {'mode': 'REQUIRED', 'aux': 'primitive'}
            element_type['type'] = types_to_bq[top.typestr]
            return element_type
        else:        
            element_type = walk_type(top.type,indent)
            element_type['mode'] = 'REPEATED'
            return element_type
    elif isinstance(top, ak.types.PrimitiveType):
        return {'type': types_to_bq[top.dtype], 'mode': 'REQUIRED', 'aux': 'primitive'}
    elif isinstance(top, ak.types.UnknownType):
        raise RuntimeError
    else:
        raise RuntimeError

def json_to_bigquery(json_schema):
    schema = []
    for x in json_schema:
        if x['type'] == 'RECORD':
            datum = bigquery.SchemaField(
                x['name'],
                x['type'],
                mode = x['mode'],
                fields = json_to_bigquery(x['fields'])
            )
            schema.append(datum)
        elif x['aux'] == 'primitive':
            datum = bigquery.SchemaField(x['name'],x['type'],x['mode'])
            schema.append(datum)
        else:
            print('wha')
    return schema

@click.group()
def main():
    pass


@main.command()
@click.argument('type_source')
@click.argument('table_name')
@click.option('--gcp-project', default = 'gke-dev-311213')
@click.option('--gcp-dataset', default = 'PHYSLITE100TB')
def create_table(type_source,table_name,gcp_project,gcp_dataset):
    e = ak.from_parquet(type_source)
    tp = ak.type(e)
    json_schema = walk_type(tp.type)
    schema = json_to_bigquery(json_schema['fields'])

    client = bigquery.Client()
    dataset_ref = bigquery.DatasetReference(gcp_project, gcp_dataset)
    table_ref = dataset_ref.table(table_name)
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table)  # API request


@main.command()
@click.argument('table_name')
@click.option('--gcp-project', default = 'gke-dev-311213')
@click.option('--gcp-dataset', default = 'PHYSLITE100TB')
def delete_table(table_name,gcp_project,gcp_dataset):
    from google.cloud import bigquery
    client = bigquery.Client()
    table_id = f'{gcp_project}.{gcp_dataset}.{table_name}'
    client.delete_table(table_id, not_found_ok=True)
    
if __name__ == '__main__':
    main()