from google.cloud import bigquery

if __name__ == '__main__':
    obj = bigquery.Client()

    # obj.create_dataset()

    # vaulted-bazaar-345605.bwt_student_dataset
    dataset = obj.dataset(dataset_id="bwt_student_dataset", project="vaulted-bazaar-345605")
    print(dataset)
    dataset.location = "US"

    final_output = obj.create_dataset(dataset=dataset)
    print("successfully created : project_id:{}, dataset:{}".format(obj.project, dataset.dataset_id))
