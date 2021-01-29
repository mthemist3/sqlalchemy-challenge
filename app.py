import pandas as pd
import numpy as np
import datetime as dt
import sqlalchemy 
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import session
from sqlalchemy import create_engine, func

import flask
from flask import Flask , jsonify


#################################################
# Database Setup
#################################################

engine = create_engine("sqlite:///hawaii.sqlite")
# reflect an existing database into a new model

Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the tables
Measurement = Base.classes.measurement
Station = Base.classes.station
session = session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################
@app.route("/")
def home():
    return (
    f"Available Routes:<br/>"
    f"Precipitation - /api/v1.0/precipitation<br/>"
    f"Stations - /api/v1.0/stations<br/>"
    f"TOBS - /api/v1.0/tobs<br/>"
    f"Calculated Temperatures (Start) - /api/v1.0/start<br/>"
    f"Calculated Temperatures (Start and End) - /api/v1.0/start/end<br/>"


# * Convert the query results to a dictionary using `date` as the key and `prcp` as the value.
# 1) Precipitation Route
@app.route("/api/v1.0/precipitation")
def precipitation():
    
    # Query dates and precip again
    session = session(engine)
    results = session.query(Measurement.date, Measurement.prcp).\
                order_by(Measurement.date).all()

    # Create a dictionary using 'date' as the key and 'prcp' as the value
    prcp_list = []

    for date, prcp in results:
        prcp_dict = {}
        prcp_dict[date] = prcp
        prcp_list.append(prcp_dict) 

    session.close()

    # Return as JSON
    return jsonify(prcp_list)
    

# * Return a JSON list of stations from the dataset.
# 2) Stations Route
@app.route("/api/v1.0/stations")
def stations():
    
    # Query all stations using distinct as a seperator
    session = session(engine)
    results = session.query(Measurement.station).distinct().all()
    
    # List the stations
    station_list = list(np.ravel(results))
    
    # Close the session
    session.close()

    # Return as JSON
    return jsonify(station_list)


# * Query the dates and temperature observations of the most active station for the last year of data.
# 3) Temperature Observation Route
@app.route("/api/v1.0/tobs")
def tobs():
    
    # Query all date info
    session = session(engine)
    dates = session.query(Measurement.date).all()
    
    # Extract and store the start and end dates of one year's data
    last_date = dates[-1][0]
    end_dt = dt.datetime.strptime(last_date, '%Y-%m-%d')
    end_dt = end_dt.date()
    start_dt = end_dt - dt.timedelta(days=365)
    
    # Query one year's worth of temperature observations
    results = session.query(Measurement.date, Measurement.tobs).\
        filter(Measurement.date>=start_dt).\
        filter(Measurement.date<=end_dt).all()
    
    # Create a dictionary using 'date' as the key and 'tobs' as the value
    tobs_list = []

    for date, tobs in results:
        tobs_dict = {}
        tobs_dict[date] = tobs
        tobs_list.append(tobs_dict) 

    session.close()

    # Return the list of dates and temperature observations
    return jsonify(tobs_list)



# 4) TMIN, TAVG, TMAX with only start date
@app.route("/api/v1.0/<start>")
def start(start):
    
    # Query dates and temperature observations
    session = session(engine)

     # Select first and last dates of the data set
    date_start = session.query(func.min(Measurement.date)).first()[0]
    date_end = session.query(func.max(Measurement.date)).first()[0]

    # Calculate temperatures if the input date is in the data set
    if start >= date_start and start <= date_end:
        calc_temps = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
            filter(Measurement.date >= start).filter(Measurement.date <= date_end).all()[0]
    
        return (
            f"Min temp: {calc_temps[0]}</br>"
            f"Avg temp: {calc_temps[1]}</br>"
            f"Max temp: {calc_temps[2]}")
    
    else:
        return jsonify({"error": f"The date {start} was not found. Please select a date between 2010-01-01 and 2017-08-23."}), 404



# 5) TMIN, TAVG, TMAX with start and end dates
@app.route("/api/v1.0/<start>/<end>")
def start_end(start, end):
    
    # Query dates and temperature observations
    session = session(engine)

    # Select first and last dates of the data set
    date_start = session.query(func.min(Measurement.date)).first()[0]
    date_end = session.query(func.max(Measurement.date)).first()[0]

    # Calculate temperatures if the input dates are in the data set
    if start >= date_start and end <= date_end:
        calc_temps = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
            filter(Measurement.date >= start).filter(Measurement.date <= end).all()[0]
    
        return (
            f"Min temp: {calc_temps[0]}</br>"
            f"Avg temp: {calc_temps[1]}</br>"
            f"Max temp: {calc_temps[2]}")
    
    else:
        return jsonify({"error": f"The dates {start} or {end} were not found. Please select dates between 2010-01-01 and 2017-08-23."}), 404
            

if __name__ == "__main__":
    app.run(debug=True)