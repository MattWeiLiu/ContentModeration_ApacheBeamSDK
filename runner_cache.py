import re
import redis
import argparse
import logging
import apache_beam as beam
import google.auth
import json
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.io import WriteToPubSub
from apache_beam.io import ReadFromPubSub
from projects.content_filter.dataflow_sdk.TopUp.pattern import patterns
from google.cloud import aiplatform
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Value

redis_host = ""
redis_port = 
redis_client = redis.Redis(host=redis_host, port=redis_port, db=0)
project = ""
location = ""
endpoint_id = ""


# Define DoFn to match keyword
class KeywordComparison(beam.DoFn):

    def process(self, data, *args, **kwargs):
        dic = json.loads(data)
        comment = dic["content"]
        cache = redis_client.get(comment)
        if not cache:
            # Regular Expression
            dic["fraud"] = 0
            for pattern in patterns:
                if re.search(pattern, comment):
                    dic["fraud"] = "Matched"
                    break
        else:
            dic["fraud"] = float(cache)
        yield dic


class Inference(beam.DoFn):

    def process(self, data, *args, **kwargs):
        comment = data["content"]
        score = self.predict(comment).predictions[0]
        data["fraud"] = score
        if score > 0.95:
            redis_client.set(comment, score)
        yield data

    def predict(self, instances):
        """
        `instances` can be either single instance of type dict or a list
        of instances.
        """
        # The AI Platform services require regional API endpoints.
        client_options = {"api_endpoint": "us-central1-aiplatform.googleapis.com"}

        # Initialize client that will be used to create and send requests.
        # This client only needs to be created once, and can be reused for multiple requests.
        client = aiplatform.gapic.PredictionServiceClient(client_options=client_options)

        # The format of each instance should conform to the deployed model's prediction input schema.
        instances = instances if type(instances) == list else [instances]
        instances = [
            json_format.ParseDict(instance_dict, Value()) for instance_dict in instances
        ]
        parameters_dict = {}
        parameters = json_format.ParseDict(parameters_dict, Value())
        endpoint = client.endpoint_path(
            project=project, location=location, endpoint=endpoint_id
        )
        response = client.predict(
            endpoint=endpoint, instances=instances, parameters=parameters
        )
        return response


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the TopUp pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_subscription',
        dest='input_subscription',
        required=True,
        help='Input Pub/Sub subscription to process.')
    parser.add_argument(
        '--output_topic',
        dest='output_topic',
        required=True,
        help='Output Pub/Sub Topic to publish result.')
    parser.add_argument(
        '--threshold',
        dest='threshold',
        required=True,
        help='Threshold')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    options = PipelineOptions(pipeline_args, streaming=True, save_main_session=save_main_session)

    # Sets the project to the default project in your current Google Cloud environment.
    # The project will be used for creating a subscription to the PubSub topic.
    _, options.view_as(GoogleCloudOptions).project = google.auth.default()

    # The pipeline will be run on exiting the with block.
    with beam.Pipeline(options=options) as p:
        # Read messages from Pub/Sub
        messages = (p | 'Read from Pub/Sub' >> ReadFromPubSub(subscription=known_args.input_subscription)
                    | 'Decode messages' >> beam.Map(lambda x: x.decode('utf-8')))

        # Cached | Matched
        comparison = (messages | 'Keyword Comparison' >> beam.ParDo(KeywordComparison()))  # float | "Matched"
        branch1 = comparison | 'Branch1' >> beam.Filter(lambda x: x["fraud"] != "Matched" and x["fraud"] > 0)
        branch2 = comparison | 'Branch2' >> beam.Filter(lambda x: x["fraud"] == "Matched")
        matched = (branch2 | 'Call external API' >> beam.ParDo(Inference()))

        # Write predictions to Pub/Sub
        merged = ((matched, branch1) | "Merge PCollections" >> beam.Flatten())

        # Call external API
        final = (merged | 'Filter' >> beam.Filter(lambda x: x["fraud"] > float(known_args.threshold)))

        (final | 'Encode messages' >> beam.Map(lambda x: json.dumps(x).encode('utf-8'))
         | 'Write to Pub/Sub Topic' >> WriteToPubSub(topic=known_args.output_topic))


if __name__ == '__main__':
    logging.basicConfig(filename="log",
                        filemode='a',
                        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.INFO)
    logging.getLogger().setLevel(logging.INFO)
    run()
