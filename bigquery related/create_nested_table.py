from google.cloud import bigquery

if __name__ == '__main__':
    obj_client = bigquery.Client()

    schema = [
        bigquery.SchemaField(name="dept_id", field_type="INTEGER"),
        bigquery.SchemaField("dept_name", "STRING"),
        bigquery.SchemaField("addresss",
                             "RECORD",
                             mode="REPEATED",
                             fields=[
                                 bigquery.SchemaField("city", "string"),
                                 bigquery.SchemaField("state", "string")
                             ])
    ]

    table = bigquery.Table("vaulted-bazaar-345605.bwt_session_dataset.dept_tb", schema)
    print("table object created: {}".format(table))

    table.clustering_fields = ["dept_id"]

    table = obj_client.create_table(table)
    print("successfully created table: {}".format(table.table_id))
