# modules to import
import config
import sqlalchemy as sqla
from sqlalchemy import create_engine
import simplejson as json
import requests
import time
from IPython.display import display

# specify rds deets
engine = create_engine(
    "mysql+mysqldb://{}:{}@{}:{}/{}".format(
        config.user, config.passwd, config.host, config.port, config.db_name),
    echo=True)

sql = """
CREATE DATABASE IF NOT EXISTS dbbikes;
"""
engine.execute(sql)

sql = """
CREATE TABLE IF NOT EXISTS station (
address VARCHAR(256),
banking INTEGER,
bike_stands INTEGER,
contract_name VARCHAR(256),
name VARCHAR(256),
number INTEGER,
position_lat REAL,
position_lng REAL,
status VARCHAR(256)
)
"""
try:
    res = engine.execute("DROP TABLE IF EXISTS station")
    res = engine.execute(sql)
    print(res.fetchall())
except Exception as e:
    print(e)

sql = """
CREATE TABLE IF NOT EXISTS availability (
number INTEGER,
available_bikes INTEGER,
available_bike_stands INTEGER,
last_update INTEGER
)
"""
try:
    res = engine.execute(sql)
    print(res.fetchall())
except Exception as e:
    print(e)

metadata = sqla.MetaData()
station = sqla.Table("station", metadata,
                     sqla.Column('address', sqla.String(256), nullable=False),
                     sqla.Column('banking', sqla.Integer),
                     sqla.Column('bike_stands', sqla.Integer),
                     sqla.Column('bonus', sqla.Integer),
                     sqla.Column('contract_name', sqla.String(256)),
                     sqla.Column('name', sqla.String(256)),
                     sqla.Column('number', sqla.Integer), sqla.Column(
                         'position_lat', sqla.REAL),
                     sqla.Column('position_lng', sqla.REAL), sqla.Column('status', sqla.String(256)))

availability = sqla.Table("availability", metadata,
                          sqla.Column('available_bikes', sqla.Integer),
                          sqla.Column('available_bike_stands', sqla.Integer),
                          sqla.Column('number', sqla.Integer),
                          sqla.Column('last_update', sqla.BigInteger)
                          )
try:
    station.drop(engine)
    availability.drop(engine)
except:
    pass

metadata.create_all(engine)


def stations_fix_keys(station):
    station['position_lat'] = station['position']['lat']
    station['position_lng'] = station['position']['lng']
    return station


stations = json.loads(open('stations.json', 'r').read())

engine.execute(station.insert(), *map(stations_fix_keys, stations))
