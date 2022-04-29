import datetime as dt
import pandas as pd
import json

from flask import Flask
"""
build_observation
- measurement_or_fact
  - dwc_intensity
  - dwc_tone
  - dwc_type
"""


def build_dwc_measure(measurement_id, measurement_type, measurement_unit, measurement_determined_date,
                      measurement_value):
    """
    "measurementID": "2022-04-24T13:43:43.893254Z",
    "measurementType": "PM1",
    "measurementUnit": "ug/m3",
    "measurementDeterminedDate": "2022-04-24T13:43:43.893254Z",
    "measurementDeterminedBy": "CanAirIO station D34TTGOT7D48E6",
    "measurementValue": 0
    """
    return {'measurementID': measurement_id,
            'measurementType': measurement_type,
            'measurementUnit': measurement_unit,
            'measurementDeterminedDate': measurement_determined_date,
            'measurementDeterminedBy': 'OdourCollect user community',
            'measurementValue': measurement_value}


def build_dwc_intensity(measure_id, datetime, intensityvalue):
    return build_dwc_measure(measure_id, 'odour', 'VDI 3882-1:1992 (odour intensity)', datetime, intensityvalue)


def build_dwc_tone(measure_id, datetime, tonevalue):
    return build_dwc_measure(measure_id, 'odour', 'VDI 3882-2:1994 (odour hedonic tone)', datetime, tonevalue)


def build_dwc_odourtype(measure_id, datetime, typevalue):
    return build_dwc_measure(measure_id, 'odour', 'Odour type', datetime, typevalue)


def build_dwc_observation(measure_id, user_id, datetime: dt.datetime, latitude, longitude, odourtype, intensity, tone):
    datetime_str = datetime.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    observation = {
        # "projects": [],
        "id": measure_id,
        "eventDate": datetime_str,  # OC observations are in UTC (.000Z)
        "created_at": datetime_str,
        "observedOn": datetime_str,
        "institutionID": "https://scienceforchange.eu",
        "collectionID": "https://odourcollect.eu",
        "institutionCode": "SfC",  # custody of data
        "collectionCode": "Odours",
        "origin": "OdourCollect",
        "datasetName": "OdourCollect observations",
        "ownerInstitutionCode": user_id,  # property of data
        "basisOfRecord": "HumanObservation",
        "type": "Event",
        "accessRights": "https://opendatacommons.org/licenses/odbl/1-0/",  # License for OC data
        "license": "ODbL v1.0",
        "rightsHolder": "Science for Change, S.L.",
        "informationWithheld": "Duration and comments are not shared. Visit https://odourcollect.eu for such details",
        "decimalLatitude": latitude,
        "decimalLongitude": longitude,
        # and now the measurementOrFact class specific properties
        "measurements": [
            build_dwc_odourtype(measure_id, datetime_str, odourtype),
            build_dwc_intensity(measure_id, datetime_str, intensity),
            build_dwc_tone(measure_id, datetime_str, tone)
        ]
    }
    return observation


app = Flask(__name__)


def load_observations(filterid=None):
    observations = pd.read_csv('odourcollect.csv')
    observations['user'] = observations['user'].apply(str)
    observations['user'] = observations['user'].apply(lambda s: 'OdourCollect user #' + s)
    observations = observations.reset_index()
    observations['published_at'] = pd.to_datetime(observations['published_at'])
    if filterid is not None:
        observations = observations['id' == filterid]
    return observations


def return_answerlist(observations):
    answerlist = []
    for index, row in observations.iterrows():
        answerlist.append(build_dwc_observation(
            row['id'],
            row['user'],
            row['published_at'],
            row['latitude'],
            row['longitude'],
            row['type'],
            row['intensity'],
            row['hedonic_tone']
        ))
    return answerlist


@app.route('/api/v1.0/observations/')
def get_full_list():
    observations = load_observations()
    return json.dumps(return_answerlist(observations))


@app.route('/api/v1.0/observations/<observationid>')
def get_single_item(observationid):
    observations = load_observations().query("id == {}".format(observationid))
    return json.dumps(return_answerlist(observations))


if __name__ == '__main__':
    app.run()

