
from google.cloud import bigquery

if __name__ == '__main__':
    bqclient = bigquery.Client()

    query="""with data_input as
            (
              select 1 as id,
              ["comedy","sports","news"] as category,
              b"lkasjdf" as byteval
            )
            select * from `vaulted-bazaar-345605.bwt_session_dataset.employee` t join data_input t1 on t.emp_id=t1.id;
            """

    query_job = bqclient.query(query)
    for row in query_job:
        print(row)
