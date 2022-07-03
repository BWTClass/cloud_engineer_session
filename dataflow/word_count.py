import logging

import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",
                        dest="input",
                        default="gs://pcm-dev-ingestion/Data/local_path/airport_data_pcm.csv",
                        help="please provide a input file")
    parser.add_argument("--output",
                        dest="output",
                        required=True,
                        help="please provide output/result location")

    known_args, pipline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        lines = p | 'Read' >> ReadFromText(known_args.input)

        counts = (
                lines | 'split' >> beam.FlatMap((lambda x: x.split(",")))
                | 'pairwithone' >> beam.Map(lambda x: (x, 1))
                | 'groupandsum' >> beam.CombinePerKey(sum)
        )

        # format result
        def format_result(word, count):
            return "%s: %d" % (word, count)

        output = counts | 'format' >> beam.MapTuple(format_result)

        output | WriteToText(known_args.output)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
