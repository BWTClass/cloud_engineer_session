from google.cloud import bigquery

if __name__ == '__main__':
    obj_client = bigquery.Client()

    schema = [
        bigquery.SchemaField(name="stud_id", field_type="INTEGER"),
        bigquery.SchemaField("stud_name", "STRING"),
        bigquery.SchemaField(name="gender", field_type="string"),
        bigquery.SchemaField(name="admission_date", field_type="DATE")
    ]

    table = bigquery.Table("vaulted-bazaar-345605.bwt_session_dataset.student_tb", schema)
    print("table object created: {}".format(table))

    table.time_partitioning = bigquery.TimePartitioning(
        field="admission_date"
    )
    table = obj_client.create_table(table)
    print("successfully created table: {}".format(table.table_id))
