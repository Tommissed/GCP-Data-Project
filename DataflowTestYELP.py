import argparse
import json
import logging

import apache_beam as beam
import pandas as pd
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage
from smart_open import open
import os

#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'project-demo-1508-9f80e4c2c1c6.json'

class ReadFile(beam.DoFn):
    def __init__(self, input_path):
        self.input_path = input_path

    def start_bundle(self):
        from google.cloud import storage
        self.client = storage.Client()

    def process(self, something):
        businessList = []
        with open (self.input_path) as f:
            for jsonObj in f:
                businessDict = json.loads(jsonObj)
                businessList.append(businessDict)

        yield businessList

class WriteCSVFile(beam.DoFn):

    def __init__(self, bucket_name):
        self.bucket_name = bucket_name

    def start_bundle(self):
        from google.cloud import storage
        self.client = storage.Client()

    def process(self, mylist):
        df = pd.DataFrame(mylist)

        bucket = self.client.get_bucket(self.bucket_name)

        bucket.blob(f"csv_exports.csv").upload_from_string(df.to_csv(index=False), 'text/csv')

class DataflowOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls,parser):
        parser.add_argument('--input_path', type=str, default='gs://textbucket-1/yelp_academic_dataset_business.json') #change bucket name here in the 'gs://...' format
        parser.add_argument('--output_bucket', type=str, default='textbucket-1') #change bucket name here in similar format to <--

def run(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    dataflow_options = pipeline_options.view_as(DataflowOptions)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (pipeline
        | 'Start' >> beam.Create([None])
        | 'Read JSON' >> beam.ParDo(ReadFile(dataflow_options.input_path))
        | 'Write CSV' >> beam.ParDo(WriteCSVFile(dataflow_options.output_bucket))
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
