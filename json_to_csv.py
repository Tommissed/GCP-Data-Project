import argparse
import json
import logging
import os

import apache_beam as beam
import pandas as pd
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage
from smart_open import open

import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)

# the "for dirname" snippet below is found at
# https://www.kaggle.com/code/udbhavsampath/udbhav-team-14-extra-credit


for dirname, _, filenames in os.walk('/archive'):
    for filename in filenames:
        print(os.path.join(dirname, filename))

# pertinent json files:

# gs://yelp-reviews-4567/archive/yelp_academic_dataset_business.json
# gs://yelp-reviews-4567/archive/yelp_academic_dataset_checkin.json
# gs://yelp-reviews-4567/archive/yelp_academic_dataset_review.json
# gs://yelp-reviews-4567/archive/yelp_academic_dataset_tip.json
# gs://yelp-reviews-4567/archive/yelp_academic_dataset_user.json
# have this Python "json_to_csv.py" run from dir 'gs://yelp-reviews-4567'


# path = '/archive/yelp_academic_dataset_checkin.json'

# '...checkin.json' **field names:** 'business_id', 'date' --> actually holds comma-separated timestamps

# checkin = pd.DataFrame()
# object_json = pd.read_json(path, lines = True, chunksize = 100000)
# for l in object_json:
#     checkin = l
#     break

# path = '/archive/yelp_academic_dataset_review.json'

# '..._review.json' **field names:** 'review_id', 'user_id', 'business_id', 'stars', 'useful', 'funny', 'cool', 'text', 'date' --> again, date here holds datetime timestamps

# review = pd.DataFrame()
# object_json = pd.read_json(path, lines=True, chunksize = 10000)
# for l in object_json:
#     review = l
#     break

# path = '/archive/yelp_academic_dataset_tip.json'

# '..._tip.json' **field names:** 'user_id', 'business_id', 'text', 'date', 'compliment_count'

# tip = pd.DateFrame()
# object_json = pd.read_json(path, lines = True, chunksize = 100000)
# for l in object_json:
#     tip = l
#     break

# path = '/archive/yelp_academic_dataset_user.json'

# '..._user.json' ** field names: ** 'user_id', 'name', 'review_count', 'yelping_since', 'useful', 'funny', 'cool', 'elite', 'friends', 'fans', 'compliment_more', 'compliment_profile', 'compliment_cute', 'compliment_list', 'compliment_note', 'compliment_plain', 'compliment_cool', 'compliment_funny', 'compliment_writer', 'compliment_photos'

# user = pd.DataFrame()
# object_json = pd.read_json(path, lines=True, chunksize = 100000)
# for l in object_json:
#     user = l
#     break


path = '/archive/yelp_academic_dataset_business.json'


business = pd.DataFrame()

object_json = pd.read_json(path, lines = True, chunksize = 100000)

for l in object_json:
    business = l
    break


class ReadFile(beam.DoFn):

    def __init__(self, input_path):
        self.input_path = input_path

    def start_bundle(self):
        from google.cloud import storage
        self.client = storage.Client()

    # "...dataset_business.json" **field names:** 'business_id', 'name', 'address', 'city', 'state', 'postal_code', 'latitude', longitude', 'stars', 'review_count', 'is_open', 'attributes', 'categories', 'hours'

    def process(self, something):
        clear_data = []
        with open(self.input_path) as fin:
            ss = fin.read()
            data = json.loads(ss)
            business = data.get('business_id')

            for line in fin:
                data = json.loads(line)
                business = data.get('business_id')

                if business and business.get('name'):
                    business_id = business
                    name = business.get('name')
                    address = business.get('address')
                    city = business.get('city')
                    state = business.get('state')
                    postal_code = business.get('postal_code')
                    stars = business.get('stars')
                    review_count = business.get('review_count')
                    is_open = bool(business.get('is_open'))
                    attributes = business.get('attributes')
                    categories = business.get('categories')
                    hours = business.get('hours')

                    clear_data.append([business_id, name, address, city, state, postal_code, stars, review_count, is_open, attributes, categories, hours])

        yield clear_data


class WriteCSVFIle(beam.DoFn):

    def __init__(self, bucket_name):
        self.bucket_name = bucket_name

    def start_bundle(self):
        self.client = storage.Client()

    def process(self, mylist):
        df = pd.DataFrame(mylist, columns={'product_id': str, 'vendor': str, 'product_type': str, 'updated_at': str, 'created_at': str, 'option_ids': str})

        bucket = self.client.get_bucket(self.bucket_name)
        bucket.blob(f"csv_exports.csv").upload_from_string(df.to_csv(index=False), 'text/csv')


class DataflowOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input_path', type=str, default='gs://alex_dataflow_temp/input.json')
        parser.add_argument('--output_bucket', type=str, default='alex_dataflow_temp')


def run(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    dataflow_options = pipeline_options.view_as(DataflowOptions)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (pipeline
         | 'Start' >> beam.Create([None])
         | 'Read JSON' >> beam.ParDo(ReadFile(dataflow_options.input_path))
         | 'Write CSV' >> beam.ParDo(WriteCSVFIle(dataflow_options.output_bucket))
         )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()