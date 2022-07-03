#!/usr/bin/env python

"""BigQuery I/O PySpark example."""

from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession \
      .builder \
      .master('yarn') \
      .config('spark.jars.packages',
               'com.google.cloud.spark:spark-bigquery-with-dependencies_2.11-0.25.0.jar') \
      .appName('spark-bigquery-demo') \
      .getOrCreate()

    # Use the Cloud Storage bucket for temporary BigQuery export data used
    # by the connector.
    bucket = "bwt-dataproc-bucket"
    spark.conf.set('temporaryGcsBucket', bucket)

    # Load data from BigQuery.
    words = spark.read.format('bigquery') \
      .option('table', 'vaulted-bazaar-345605.bwt_session_dataset.employee') \
      .load()
    words.createOrReplaceTempView('words')

    # Perform word count.
    word_count = spark.sql(
        'SELECT word, SUM(word_count) AS word_count FROM words GROUP BY word')
    word_count.show()
    word_count.printSchema()

    # # Saving the data to BigQuery
    # word_count.write.format('bigquery') \
    #   .option('table', 'bwt_session_dataset.shakespeare') \
    #   .save()