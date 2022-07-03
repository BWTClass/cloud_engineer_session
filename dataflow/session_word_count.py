import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
import logging

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--inputgcs",
                        dest="inputgcs",
                        required=True,
                        help="Please provide Input GCS path")
    parser.add_argument("--outputpath",
                        dest="outputpath",
                        required=True,
                        help="Please provide Output GCS path")
    known_args, pipeline_args = parser.parse_known_args()

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True


    def test(element):
        print("=========================")
        print(element)
        print("=========================")
        return element


    with beam.Pipeline(options=pipeline_options) as p:
        line = (p | 'Read' >> ReadFromText(known_args.inputgcs)
                # | "test1" >> beam.Map(test)
                )
        sumval = (line
                  | "split" >> beam.FlatMap(lambda x: x.split(","))
                  # | "test0" >> beam.Map(test)
                  | "pairwithone" >> beam.Map(lambda x: (x, 1))
                  | 'valuesum' >> beam.CombinePerKey(sum)
                  )


        def format_ele(word, cnt):
            return "{}: {}".format(word, cnt)


        final = (sumval | "format" >> beam.MapTuple(format_ele)
                 | "write" >> WriteToText(known_args.outputpath)
                 )

# --inputgcs gs://shubh_bucket-1/Survey_Project/Survey_Data.csv --outputpath gs://shubh_bucket-1/session_output/
