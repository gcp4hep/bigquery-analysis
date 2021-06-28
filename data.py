import pyarrow.parquet as pq
import awkward as ak
import json
import click
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

@click.group()
def main():
    pass

@main.command()
@click.argument('parquet-file')
@click.argument('jsonl-file')
@click.option('--max-events', default = None)
def prep_jsonl(parquet_file,jsonl_file,max_events):
    data = ak.from_parquet(parquet_file)

    sl = slice(None,max_events)
    what = json.loads(ak.to_json(data[sl]))

    with open(jsonl_file,'w') as f:
        for d in what:
            for k in list(d.keys()):
                if k in ['TrigMatchedObjects']:
                    d.pop(k)
            d['METAssoc_MET'] = []
            f.write(f'{json.dumps(d)}'+'\n')


@main.command()
@click.argument('jsonl-file')
@click.argument('table_name')
@click.option('--gcp-project', default = 'gke-dev-311213')
@click.option('--gcp-dataset', default = 'PHYSLITE100TB')
@click.option('--local/--remote', default = 'PHYSLITE100TB')
def load_data(jsonl_file,table_name,gcp_project,gcp_dataset,local):
    client = bigquery.Client()

    table_id = f"{gcp_project}.{gcp_dataset}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    if local:
        with open(jsonl_file, "rb") as source_file:
            load_job = client.load_table_from_file(
                source_file, table_id, job_config=job_config
            )
    else:
        uri = jsonl_file
        load_job = client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )

    try:
        load_job.result()
    except BadRequest as ex:
        for err in ex.errors:
            print(err)

    destination_table = client.get_table(table_id)
    click.secho("Table now has {} rows.".format(destination_table.num_rows))


if __name__ == '__main__':
    main()