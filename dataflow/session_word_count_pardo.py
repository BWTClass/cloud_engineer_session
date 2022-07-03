import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
import logging


class FormatDoFn(beam.DoFn):
    def process(self, element):
        return [{"word": str(element[0]),
                 "count": element[1]}]


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--inputgcs",
                        dest="inputgcs",
                        required=True,
                        help="Please provide Input GCS path")
    parser.add_argument("--output",
                        dest="output",
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


    tbl_schema = ("word:string, count:integer")

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


        final = (sumval | "format" >> beam.ParDo(FormatDoFn())
                 | 'writetobigquery' >> beam.io.WriteToBigQuery(
                    table=known_args.output,
                    schema=tbl_schema,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
                )
                 # | "write" >> WriteToText(known_args.outputpath)
                 )

# --inputgcs gs://shubh_bucket-1/Survey_Project/Survey_Data.csv --outputpath gs://shubh_bucket-1/session_output/

# fiery-cabinet-350912:shubh_dataset2.wordcount