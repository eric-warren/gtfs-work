from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, ForeignKey, Date, Time, TIMESTAMP, LargeBinary
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Cache(Base):
    __tablename__ = 'cache'

    id = Column(Integer, primary_key=True)
    key = Column(String, unique=True, nullable=False)
    value = Column(LargeBinary, nullable=False)
    timestamp = Column(Integer, nullable=False)

class Stop(Base):
    __tablename__ = 'stops'
    id = Column(Integer, primary_key=True, autoincrement=True)
    stop_id = Column(String)  
    stop_code = Column(Integer)  # Added to store stop_code
    stop_name = Column(String)
    stop_desc = Column(String)
    tts_stop_name = Column(String)
    stop_lat = Column(Float)
    stop_lon = Column(Float)
    zone_id = Column(Integer)  # Changed from Integer to String to accommodate empty or non-numeric values
    stop_url = Column(String)
    location_type = Column(Integer)
    parent_station = Column(String)  # Changed from Integer to String to accommodate empty values
    stop_timezone = Column(String)
    wheelchair_boarding = Column(Integer)
    level_id = Column(String)  # Added to store level_id
    platform_code = Column(String)  # Added to store platform_code

class Route(Base):
    __tablename__ = 'routes'
    id = Column(Integer, primary_key=True, autoincrement=True)
    route_id = Column(Integer)
    agency_id = Column(Integer)
    route_short_name = Column(String)
    route_long_name = Column(String)
    route_desc = Column(String)
    route_type = Column(Integer)
    route_url = Column(String)
    route_color = Column(String)
    route_text_color = Column(String)

class Trip(Base):
    __tablename__ = 'trips'
    id = Column(Integer, primary_key=True, autoincrement=True)
    trip_id = Column(String)
    route_id = Column(Integer, ForeignKey('routes.route_id'))
    service_id = Column(Integer, ForeignKey('calendar.service_id'))
    trip_headsign = Column(String)
    trip_short_name = Column(String)
    direction_id = Column(Integer)
    block_id = Column(Integer)
    shape_id = Column(String, ForeignKey('shapes.shape_id'))
    wheelchair_accessible = Column(Integer)
    bikes_allowed = Column(Integer)

class StopTime(Base):
    __tablename__ = 'stop_times'
    id = Column(Integer, primary_key=True, autoincrement=True)
    trip_id = Column(String, ForeignKey('trips.trip_id'))
    arrival_time = Column(Time)
    departure_time = Column(Time)
    stop_id = Column(String, ForeignKey('stops.stop_id'))
    stop_sequence = Column(Integer)
    stop_headsign = Column(String)
    pickup_type = Column(Integer)
    drop_off_type = Column(Integer)
    shape_dist_traveled = Column(Float)
    timepoint = Column(Integer)

class Calendar(Base):
    __tablename__ = 'calendar'
    id = Column(Integer, primary_key=True, autoincrement=True)
    service_id = Column(Integer)
    monday = Column(Integer)
    tuesday = Column(Integer)
    wednesday = Column(Integer)
    thursday = Column(Integer)
    friday = Column(Integer)
    saturday = Column(Integer)
    sunday = Column(Integer)
    start_date = Column(Date)
    end_date = Column(Date)

class CalendarDate(Base):
    __tablename__ = 'calendar_dates'
    id = Column(Integer, primary_key=True, autoincrement=True)
    service_id = Column(Integer, ForeignKey('calendar.service_id'))
    date = Column(Date)
    exception_type = Column(Integer)

class Shape(Base):
    __tablename__ = 'shapes'
    id = Column(Integer, primary_key=True, autoincrement=True)
    shape_id = Column(String)  # Change from Integer to String
    shape_pt_lat = Column(Float)
    shape_pt_lon = Column(Float)
    shape_pt_sequence = Column(Integer)
    shape_dist_traveled = Column(Float)


class TripUpdate(Base):
    __tablename__ = 'trip_updates'
    id = Column(Integer, primary_key=True, autoincrement=True)
    update_id = Column(String)
    is_deleted = Column(Boolean)
    trip_id = Column(String, ForeignKey('trips.trip_id'))
    route_id = Column(String, ForeignKey('routes.route_id'))
    direction_id = Column(Integer)
    start_time = Column(String)
    start_date = Column(String)
    schedule_relationship = Column(Integer)
    vehicle_id = Column(String)
    vehicle_label = Column(String)
    vehicle_license_plate = Column(String)
    timestamp = Column(TIMESTAMP)
    delay = Column(Integer)

class StopTimeUpdate(Base):
    __tablename__ = 'stop_time_updates'
    id = Column(Integer, primary_key=True, autoincrement=True)
    trip_update_id = Column(Integer, ForeignKey('trip_updates.id'))
    stop_sequence = Column(Integer)
    stop_id = Column(String, ForeignKey('stops.stop_id'))
    arrival_time = Column(TIMESTAMP)
    departure_time = Column(TIMESTAMP)
    schedule_relationship = Column(Integer)

class VehiclePosition(Base):
    __tablename__ = 'vehicle_positions'
    id = Column(Integer, primary_key=True, autoincrement=True)
    update_id = Column(String)
    is_deleted = Column(Boolean)
    trip_id = Column(String, ForeignKey('trips.trip_id'))
    route_id = Column(String, ForeignKey('routes.route_id'))
    start_time = Column(String)
    start_date = Column(String)
    schedule_relationship = Column(Integer)
    vehicle_id = Column(String)
    vehicle_label = Column(String)
    vehicle_license_plate = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    bearing = Column(Float)
    odometer = Column(Float)
    speed = Column(Float)
    timestamp = Column(TIMESTAMP)
    congestion_level = Column(Integer)
    occupancy_status = Column(Integer)
    occupancy_percentage = Column(Integer)
    current_stop_sequence = Column(Integer)
    stop_id = Column(String, ForeignKey('stops.stop_id'))
    current_status = Column(Integer)