import json
import os

__file_path = os.path.abspath(__file__)
__dir_path = os.path.dirname(__file_path)

DICTIONARY_LOC = __dir_path
DICTIONARY_FILE = "personicle_data_types.json"

def find_datastream(personicle_data_type):
    with open(os.path.join(DICTIONARY_LOC, DICTIONARY_FILE), "r") as fp:
        PERSONICLE_DATA_DICTIONARY = json.load(fp)
        try:
            return PERSONICLE_DATA_DICTIONARY["com.personicle"]["individual"]["datastreams"].get(personicle_data_type, None)
        except KeyError as e:
            raise Exception("Personicle Data dictionary file formatted incorrectly or missing")

def find_event_definition(personicle_event_type):
    with open(os.path.join(DICTIONARY_LOC, DICTIONARY_FILE), "r") as fp:
        PERSONICLE_DATA_DICTIONARY = json.load(fp)
        try:
            return PERSONICLE_DATA_DICTIONARY["com.personicle"]["individual"]["events"].get(personicle_event_type, None)
        except KeyError as e:
            raise Exception("Personicle Data dictionary file formatted incorrectly or missing")
