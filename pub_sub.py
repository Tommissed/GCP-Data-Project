import argparse
import json
import logging

import apache_beam as beam
import pandas as pd
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage
from smart_open import open

# pertinent json files:

# gs://yelp-reviews-4567/archive/yelp_academic_dataset_business.json
# gs://yelp-reviews-4567/archive/yelp_academic_dataset_checkin.json
# gs://yelp-reviews-4567/archive/yelp_academic_dataset_review.json
# gs://yelp-reviews-4567/archive/yelp_academic_dataset_tip.json
# gs://yelp-reviews-4567/archive/yelp_academic_dataset_user.json
# have this Python "json_to_csv.py" run from dir 'gs://yelp-reviews-4567'

    # "...dataset_business.json" **field names:** 'business_id', 'name', 'address', 'city', 'state', 'postal_code', 'latitude', longitude', 'stars', 'review_count', 'is_open', 'attributes', 'categories', 'hours'

class ReadFile(beam.DoFn):

    def __init__(self, input_path):
        self.input_path = input_path
    
    def start_bundle(self):
        self.client = storage.Client()

    def process(self, something):
        clear_data = []
        with open(self.input_path) as fin:
            for line in fin:
                data = json.loads(line)
                business = data.get('business')

                if business and business.get('business_id'):
                    business_id = str(business.get('business_id'))
                    name = str(business.get('name'))
                    address = str(business.get('address'))
                    city = str(business.get('city'))
                    state = str(business.get('state'))
                    postal_code = str(business.get('postal_code'))
                    latitude = str(business.get('latitude'))
                    longitude = str(business.get('longitude'))
                    stars = int(business.get('stars'))
                    review_count = int(business.get('review_count'))
                    is_open = bool(business.get('is_open'))
                    attributes = dict(business.get('attributes'))
                    categories = str(business.get('categories'))
                    hours = str(business.get('hours'))

                    clear_data.append([business_id, name, address, city, state, postal_code, latitude, longitude, stars, review_count, is_open, attributes, categories, hours])
        yield clear_data

class WriteCSVFile(beam.DoFn):

    def __init__(self, bucket_name):
        self.bucket_name = bucket_name

    def start_bundle(self):
        self.client = storage.Client()

    def process(self, mylist):
        df = pd.DataFrame(mylist, columns = {'business_id': str, 'name': str, 'address': str, 'city': str, 'state': str, 'postal_code': str, 'latitude': str, 'longitude': str, 'stars': int, 'review_count': int, 'is_open': bool, 'attributes': dict, 'categories': str, 'hours': str})

        bucket = self.client.get_bucket(self.bucket_name)

        bucket.blob(f'csv_businessjson_exports.csv').upload_from_string(df.to_csv(index=False), 'text/csv')

class DataflowOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--input_path',
            type=str,
            default='gs://yelp-reviews-4567/archive/yelp_academic_dataset_business.json'
        )

        parser.add_argument(
            '--output_bucket', type=str, default='yelp-reviews-4567'
        )

def run(argv=None, save_main_session=True):
    # need save_main_session toggled to True so that one or more DoFn(s) can rely on the global context

    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    # pipeline_options.view_as(DataflowOptions).save_main_session = save_main_session
    dataflow_options = pipeline_options.view_as(DataflowOptions)

    with beam.Pipeline(options=pipeline_options) as p:
        (p
        | 'Start' >> beam.Create([None])
        # | 'ReadFile' >> ReadFromText(known_args.input)
        | 'ReadJson' >> beam.ParDo(ReadFile(dataflow_options.input_path))
        | 'WriteCSV' >> beam.ParDo(WriteCSVFile(dataflow_options.output_bucket))
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()