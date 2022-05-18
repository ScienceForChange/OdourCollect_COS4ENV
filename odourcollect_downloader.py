import os
from csv import QUOTE_NONNUMERIC
import json
import pandas as pd
from pydantic import (BaseModel, conint, root_validator, validator, ValidationError)
from typing import Optional
from datetime import date, datetime, time
import requests

OC_ENDPOINT = 'https://odourcollect.eu/api/odor/list'

TYPE_LIST = {1: 'Waste|Fresh waste',
             2: 'Waste|Decomposed waste',
             3: 'Waste|Leachate',
             4: 'Waste|Biogas',
             5: 'Waste|Biofilter',
             6: 'Waste|Ammonia',
             7: 'Waste|Amines',
             8: 'Waste|Other',
             9: 'Waste|I don\'t know',
             10: 'Waste Water|Waste water',
             11: 'Waste Water|Rotten eggs',
             12: 'Waste Water|Sludge',
             13: 'Waste Water|Chlorine',
             14: 'Waste Water|Other',
             15: 'Waste Water|I don\'t know',
             16: 'Agriculture / Livestock|Dead animal',
             17: 'Agriculture / Livestock|Cooked meat',
             18: 'Agriculture / Livestock|Organic fertilizers (manure/slurry)',
             19: 'Agriculture / Livestock|Animal feed',
             20: 'Agriculture / Livestock|Cabbage soup',
             21: 'Agriculture / Livestock|Rotten eggs',
             22: 'Agriculture / Livestock|Ammonia',
             23: 'Agriculture / Livestock|Amines',
             24: 'Agriculture / Livestock|Other',
             25: 'Agriculture / Livestock|I don\'t know',
             26: 'Food Industries|Fat / Oil',
             27: 'Food Industries|Coffee',
             28: 'Food Industries|Cocoa',
             29: 'Food Industries|Milk / Dairy',
             30: 'Food Industries|Animal food',
             31: 'Food Industries|Ammonia',
             32: 'Food Industries|Malt / Hop',
             33: 'Food Industries|Fish',
             34: 'Food Industries|Bakeries',
             35: 'Food Industries|Raw meat',
             36: 'Food Industries|Ammines',
             37: 'Food Industries|Cabbage soup',
             38: 'Food Industries|Rotten eggs',
             39: 'Food Industries|Bread / Cookies',
             40: 'Food Industries|Alcohol',
             41: 'Food Industries|Aroma / Flavour',
             42: 'Food Industries|Other',
             43: 'Food Industries|I don\'t know',
             44: 'Industrial|Cabbage soup',
             45: 'Industrial|Oil / Petrochemical',
             46: 'Industrial|Gas',
             47: 'Industrial|Asphalt / Rubber',
             48: 'Industrial|Chemical',
             49: 'Industrial|Ammonia',
             50: 'Industrial|Leather',
             51: 'Industrial|Metal',
             52: 'Industrial|Plastic',
             53: 'Industrial|Sulphur',
             54: 'Industrial|Alcohol',
             55: 'Industrial|Ketone / Ester / Acetate / Ether',
             56: 'Industrial|Amines',
             57: 'Industrial|Glue / Adhesive',
             58: 'Urban|Urine',
             59: 'Urban|Traffic',
             60: 'Urban|Sewage',
             61: 'Urban|Waste bin',
             62: 'Urban|Waste truck',
             63: 'Urban|Sweat',
             64: 'Urban|Cannabis',
             65: 'Urban|Fresh grass',
             66: 'Urban|Humidity / Wet soil',
             67: 'Urban|Flowers',
             68: 'Urban|Food',
             69: 'Urban|Chimney (burnt wood)',
             70: 'Urban|Paint',
             71: 'Urban|Fuel',
             72: 'Urban|Other',
             73: 'Urban|I don\'t know',
             74: 'Nice|Flowers',
             75: 'Nice|Food',
             76: 'Nice|Bread / Cookies',
             77: 'Nice|Fruit',
             78: 'Nice|Fresh grass',
             79: 'Nice|Forest / Trees / Nature',
             80: 'Nice|Mint / Rosemary / Lavander',
             81: 'Nice|Sea',
             82: 'Nice|Perfume',
             83: 'Nice|Chimney (burnt wood)',
             84: 'Nice|Wood',
             85: 'Nice|New book',
             86: 'Nice|Other',
             87: 'Nice|I don\'t know',
             88: 'No Odour|No Odour',
             89: 'Other|NA'}

CATEGORY_LIST = {1: 'Waste related odours',
                 2: 'Waste water related odours',
                 3: 'Agriculture and livestock related odours',
                 4: 'Food Industries related odours',
                 5: 'Industry related odours',
                 6: 'Urban odours',
                 7: 'Nice odours',
                 8: 'Other odours not fitting elsewhere',
                 9: 'No odour observations (for testing, for reporting the end of an odour, etc.)'}

ANNOY_ID_TO_REAL_NUMBER = {1: -4,
                           2: -3,
                           3: -2,
                           4: -1,
                           5: 0,
                           6: 1,
                           7: 2,
                           8: 3,
                           9: 4}

ANNOY_ID_TO_DESCRIPTION = {1: 'Extremely unpleasant',
                           2: 'Very unpleasant',
                           3: 'Unpleasant',
                           4: 'Slightly unpleasant',
                           5: 'Neutral',
                           6: 'Slightly pleasant',
                           7: 'Pleasant',
                           8: 'Very pleasant',
                           9: 'Extremely pleasant'}

INTENSITY_ID_TO_REAL_NUMBER = {1: 0,
                               2: 1,
                               3: 2,
                               4: 3,
                               5: 4,
                               6: 5,
                               7: 6}

INTENSITY_ID_TO_DESCRIPTION = {1: 'Not perceptible',
                               2: 'Very weak',
                               3: 'Weak',
                               4: 'Noticeable',
                               5: 'Strong',
                               6: 'Very strong',
                               7: 'Extremely strong'}

DURATION_LIST = {0: '(No odour)',
                 1: 'Punctual',
                 2: 'Continuous in the last hour',
                 3: 'Continuous throughout the day'}


def day_of_week(whatdate: date) -> str:
    weekdays = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
    dow = whatdate.weekday()
    return weekdays[dow]


class Odour(BaseModel):
    id: int
    userid: int
    category: str
    type: str
    intensity: str
    annoy: str
    duration: str
    observeddatetime: datetime
    observedtimeonly: time
    latitude: float
    longitude: float


class OdourType(BaseModel):
    id: int
    category: str
    type: str


class OdourIntensity(BaseModel):
    id: int
    value: int
    desc: str


class OdourAnnoy(BaseModel):
    id: int
    value: int
    desc: str


class OdourDuration(BaseModel):
    id: int
    desc: str


class OCRequest(BaseModel):
    type: Optional[conint(ge=0, le=9)]  # OdourCollect's odour type (called "category" here). 0 = All, 1-88 = filters
    subtype: Optional[conint(ge=0, le=89)]  # OdourCollect's odour subtype (called "type" here). 0 = All, 1-9 = filters
    minAnnoy: Optional[conint(ge=-4, le=4)]  # OdourCollect's "hedonic tone", from -4 to 4. 0 = neutral.
    maxAnnoy: Optional[conint(ge=-4, le=4)]
    minIntensity: Optional[conint(ge=0, le=6)]  # "intensity" in OdourCollect, from 0 to 6
    maxIntensity: Optional[conint(ge=0, le=6)]
    date_init: Optional[date]  # yyyy-mm-dd
    date_end: Optional[date]  # yyyy-mm-dd

    @root_validator()
    def validate_ocrequest(cls, values):
        if values.get('minannoy') and values.get('maxannoy'):
            if values.get('minannoy') > values.get('maxannoy'):
                raise ValueError('Min annoy can\'t be greater than max annoy')
        if values.get('minintensity') and values.get('maxintensity'):
            if values.get('minintensity') > values.get('maxintensity'):
                raise ValueError('Min intensity can\'t be greater than max intensity')
        if values.get('date_init') and values.get('date_end'):
            if values.get('date_init') > values.get('date_end'):
                raise ValueError('Starting date can\'t be later than ending date')
        return values


class GPScoords(BaseModel):
    lat: float
    long: float

    @validator('lat')
    def validate_lat(cls, v):
        # print('Validating: {}'.format(v))
        if v < -90.0 or v > 90.0:
            raise ValidationError(f'Incorrect GPS latitude value detected: {v}')
        return v

    @validator('long')
    def validate_long(cls, v):
        # print('Validating: {}'.format(v))
        if v < -180.0 or v > 180.0:
            raise ValidationError(f'Incorrect GPS longitude value detected: {v}')
        return v


def build_df(json_response) -> pd.DataFrame:
    observationslist = []
    try:
        observationslist = json.loads(json_response)['content']
    except KeyError:
        print('Received JSON data does not have a "content" key:')
        print(json_response)
        exit(2)
    if len(observationslist) == 0:
        print('No data for criteria specified')
        exit(1)
    ocdf = pd.DataFrame(observationslist)

    # DATA TRANSFORMS
    # USERS: adds the character "u" as a prefix for the user ID number so they clearly become categoric, not numeric
    ocdf['id_user'] = ocdf['id_user'].apply(str)
    # ocdf['id_user'] = ocdf['id_user'].apply(lambda s: '' + s)
    ocdf.rename(columns={'id_user': 'user'}, inplace=True)

    ocdf['category'] = ocdf['id_odor_type']
    ocdf.replace(inplace=True, to_replace={'category': TYPE_LIST})
    ocdf[['category', 'type']] = ocdf['category'].str.split('|', n=1, expand=True)

    ocdf['hedonic_tone'] = ocdf['id_odor_annoy']
    ocdf.replace(inplace=True, to_replace={'hedonic_tone': ANNOY_ID_TO_DESCRIPTION})

    ocdf['intensity'] = ocdf['id_odor_intensity']
    ocdf.replace(inplace=True, to_replace={'intensity': INTENSITY_ID_TO_DESCRIPTION})

    # And reorder fields
    ocdf = ocdf[
        ['id', 'user', 'published_at', 'type', 'hedonic_tone', 'intensity', 'latitude', 'longitude']]

    return ocdf


def get_oc_data():
    r = requests.post(OC_ENDPOINT, verify=True)
    if r.status_code != 200:
        print(f'Unexpected HTTP code received: {r.status_code}')
        exit(1)
    ocdf = build_df(r.text)
    return ocdf


if __name__ == '__main__':
    print('Getting latest OdourCollect data...')
    df = get_oc_data()
    print('Saving obtained data to file odourcollect.csv...')
    df.to_csv('odourcollect-temp.csv', quoting=QUOTE_NONNUMERIC, index=False)
    if os.path.exists('odourcollect.csv'):
        os.remove('odourcollect.csv')
    os.rename('odourcollect-temp.csv', 'odourcollect.csv')
