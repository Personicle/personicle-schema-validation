from flask import Flask, Response, jsonify, request
import logging
import os
from fastavro import validate, parse_schema
import json
from data_dictionary import find_datastream, find_event_definition

Log_Format = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(filename = "data_dictionary.log",
                    filemode = "w",
                    format = Log_Format,
                    level=logging.DEBUG
                    )
logger = logging.getLogger(__name__) 

script_dir = os.path.dirname(__file__)

app = Flask()

@app.route("/")
def test_application():
    return Response("Testing schema validation application")


@app.route("/validate-data-packet", ["POST"])
def validate_data_packet():
    data_type = request.args.get("data_type")
    current_event = request.get_json()['data']
    # current_event = event.body_as_json(encoding='UTF-8')
    try:
        logger.info("Received data packet for data type {}:\n{}".format(data_type, json.dumps(current_event, indent=2)))

        stream_type = current_event['streamName']
        
        # match the stream name to the data dictionary
        if data_type == "event":
            stream_information = find_event_definition(stream_type)
        elif data_type == "datastream":
            stream_information = find_datastream(stream_type)
        else:
            logger.warn("Incorectly formatted data, data type not found in data dictionary: {}".format(json.dumps(current_event, indent=2)))
            return Response("Incorectly formatted data, data type not found in data dictionary", 403)
        

        # stream_information = match_data_dictionary(stream_type)
        # get the corresponding schema and the table
        file_path = os.path.join(script_dir, f"avro_modules/{stream_information['base_schema']}")
        
        with open(file_path, 'r') as fi:
            schema = json.load(fi)
        
        # validate the event with avro schema
        parsed_schema = parse_schema(schema) 
        if validate(current_event, parsed_schema, raise_errors=False):
            logger.info("Valid event")
            return Response({"schema_check": True})
    except KeyError:
        return Response("Incorrect packet format", 403)


@app.route("/match-data-dictionary")
def match_data_dictionary():
    data_type = request.args.get("data_type")
    stream_type = request.args.get("stream_name")
    if data_type == "event":
        stream_information = find_event_definition(stream_type)
    elif data_type == "datastream":
        stream_information = find_datastream(stream_type)
    else:
        logger.warn("Incorectly formatted data, data type not found in data dictionary: {}".format(json.dumps(current_event, indent=2)))
        return Response("Incorectly formatted data, data type not found in data dictionary", 403)
    return jsonify(stream_information)