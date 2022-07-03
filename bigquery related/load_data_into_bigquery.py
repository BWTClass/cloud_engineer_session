from google.cloud import bigquery

if __name__ == '__main__':
    obj_client = bigquery.Client()

    load_conf = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("dept_id", "integer"),
            bigquery.SchemaField("dept_name", "string"),
            bigquery.SchemaField("department_date", "timestamp"),
        ],
        skip_leading_rows=1,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.YEAR,
            field="department_date"
        )
    )

    gcs_path = "gs://bwt-session-bucket/input_data/department.csv"

    load_job = obj_client. \
        load_table_from_uri(project="vaulted-bazaar-345605",
                            source_uris=gcs_path,
                            destination="bwt_session_dataset.department",
                            location="US",
                            job_config=load_conf,
                            job_id_prefix="loadcsv")

    load_job.result()

    table = obj_client.get_table("bwt_session_dataset.department")
    print("successfully load data into bigquery table: {}, number of records loaded: {}".format(
        "bwt_session_dataset.department",
        table.num_rows))
