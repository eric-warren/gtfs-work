import requests
from google.transit import gtfs_realtime_pb2
import zipfile
import io
import time
import csv
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from db import VehiclePosition, TripUpdate, Base, Cache, StopTimeUpdate, Stop, Trip, Shape, Route, CalendarDate, Calendar, StopTime
from datetime import datetime
import logging
from config import SUBSCRIPTION_KEY
logging.basicConfig(level=logging.INFO)
logging.basicConfig(format="%(levelname)s:%(name)s:%(message)s")


# Create an engine and session
# The engine is responsible for managing the connection to the database.
# Here, we are using SQLite as the database, and the database file is named 'gtfs_cache.db'.
logging.info("Creating database engine and initializing session.")
engine = create_engine('sqlite:///gtfs_cache.db')

# Create all tables defined in the Base metadata if they don't exist.
# This ensures that the database schema is up-to-date with the models.
Base.metadata.create_all(engine)

# Create a configured "Session" class and a session instance.
# The session is used to interact with the database, allowing us to query and persist data.
Session = sessionmaker(bind=engine)
logging.info("Database engine created and session initialized.")



def convert_to_time(time_str):
    if type(time_str) == str:
        # Check if the hour part exceeds 23 and adjust the time
        hours, minutes, seconds = map(int, time_str.split(':'))
        if hours >= 24:
            hours -= 24
        # Reformat the adjusted time as a string
        adjusted_time_str = f"{hours:02}:{minutes:02}:{seconds:02}"
        return datetime.strptime(adjusted_time_str, "%H:%M:%S").time()
    else:
        return time_str

def parseAndStoreStopTimes(session, stop_times_data):
    """
    Parses the stop_times.txt data and stores it in the database.

    This function reads the stop_times data, parses each line to extract the relevant fields,
    and stores them in the StopTime table in the database.

    Args:
        session (Session): The database session used for querying and storing data.
        stop_times_data (str): The content of the stop_times.txt file as a string.

    Returns:
        None
    """
    logging.info("Parsing and storing stop times data.")

    # Split the data into lines
    lines = stop_times_data.splitlines()
    # Extract the header to determine the column indices
    header = lines[0].split(',')
    trip_id_index = header.index('trip_id')
    arrival_time_index = header.index('arrival_time')
    departure_time_index = header.index('departure_time')
    stop_id_index = header.index('stop_id')
    stop_sequence_index = header.index('stop_sequence')
    stop_headsign_index = header.index('stop_headsign')
    pickup_type_index = header.index('pickup_type')
    drop_off_type_index = header.index('drop_off_type')
    shape_dist_traveled_index = header.index('shape_dist_traveled')
    timepoint_index = header.index('timepoint')

    new_stop_times = []
    processed_count = 0

    for line in lines[1:]:  # Skip the header line
        # Split each line into fields
        fields = line.split(',')
        
        # Handle empty strings by replacing them with None
        trip_id = fields[trip_id_index] if fields[trip_id_index] else None
        arrival_time = convert_to_time(fields[arrival_time_index]) if fields[arrival_time_index] else None
        departure_time = convert_to_time(fields[departure_time_index]) if fields[departure_time_index] else None
        stop_id = fields[stop_id_index] if fields[stop_id_index] else None
        stop_sequence = int(fields[stop_sequence_index]) if fields[stop_sequence_index] else None
        stop_headsign = fields[stop_headsign_index] if fields[stop_headsign_index] else None
        pickup_type = int(fields[pickup_type_index]) if fields[pickup_type_index] else None
        drop_off_type = int(fields[drop_off_type_index]) if fields[drop_off_type_index] else None
        shape_dist_traveled = float(fields[shape_dist_traveled_index]) if fields[shape_dist_traveled_index] else None
        timepoint = int(fields[timepoint_index]) if fields[timepoint_index] else None

        # Create a new StopTime entry
        new_stop_time = StopTime(
            trip_id=trip_id,
            arrival_time=convert_to_time(arrival_time),
            departure_time=(departure_time),
            stop_id=stop_id,
            stop_sequence=stop_sequence,
            stop_headsign=stop_headsign,
            pickup_type=pickup_type,
            drop_off_type=drop_off_type,
            shape_dist_traveled=shape_dist_traveled,
            timepoint=timepoint
        )
        new_stop_times.append(new_stop_time)

        # Print progress every 1000 entries
        processed_count += 1
        if processed_count % 1000 == 0:
            logging.info(f"Processed {processed_count} stop times.")

    # Bulk insert new stop times
    if new_stop_times:
        session.bulk_save_objects(new_stop_times)
        logging.info(f"Inserted {len(new_stop_times)} new stop times.")

    # Commit all changes at once
    session.commit()
    logging.info('Stop times data stored successfully.')





def storeTripUpdates(session, trip_updates, batch_size=1000):
    """
    Stores trip update data into the database.

    This function processes the provided trip update data, checks if each update already exists
    in the database, and either updates the existing entry or creates a new one.

    Args:
        session (Session): The database session used for querying and storing data.
        trip_updates (object): The GTFS-RT trip updates object, containing trip entities.
        batch_size (int): Number of records to process before committing and clearing the session.

    Returns:
        None
    """
    logging.info('Parsing and storing trip updates data.')

    # Initialize counters and buffers
    processed_count = 0
    new_updates = []

    # Pre-fetch existing trip updates into a dictionary for fast look-up
    logging.info('Pre-fetching existing trip updates...')
    existing_updates = {
        update.trip_id: update
        for update in session.query(TripUpdate).all()
    }
    logging.info(f'Loaded {len(existing_updates)} existing trip updates.')

    # Process each entity in the trip updates
    for entity in trip_updates.entity:
        trip_update = entity.trip_update
        timestamp = datetime.fromtimestamp(trip_update.timestamp)

        existing_update = existing_updates.get(trip_update.trip.trip_id)

        if existing_update:
            # Update existing trip update fields
            existing_update.update_id = entity.id
            existing_update.route_id = trip_update.trip.route_id
            existing_update.direction_id = trip_update.trip.direction_id
            existing_update.start_time = trip_update.trip.start_time
            existing_update.start_date = trip_update.trip.start_date
            existing_update.schedule_relationship = trip_update.trip.schedule_relationship
            existing_update.vehicle_id = trip_update.vehicle.id
            existing_update.vehicle_label = trip_update.vehicle.label
            existing_update.vehicle_license_plate = trip_update.vehicle.license_plate
            existing_update.delay = trip_update.delay
            existing_update.timestamp = timestamp
        else:
            # Create a new TripUpdate entry
            new_update = TripUpdate(
                update_id=entity.id,
                trip_id=trip_update.trip.trip_id,
                route_id=trip_update.trip.route_id,
                direction_id=trip_update.trip.direction_id,
                start_time=trip_update.trip.start_time,
                start_date=trip_update.trip.start_date,
                schedule_relationship=trip_update.trip.schedule_relationship,
                vehicle_id=trip_update.vehicle.id,
                vehicle_label=trip_update.vehicle.label,
                vehicle_license_plate=trip_update.vehicle.license_plate,
                timestamp=timestamp,
                delay=trip_update.delay
            )
            new_updates.append(new_update)

        # Process stop time updates efficiently
        storeStopTimeUpdates(session, existing_update.trip_id if existing_update else trip_update.trip.trip_id, trip_update.stop_time_update)

        # Commit in batches to reduce session size
        processed_count += 1
        if processed_count % batch_size == 0:
            if new_updates:
                session.bulk_save_objects(new_updates)
                new_updates.clear()  # Clear the list to avoid memory buildup
            session.commit()
            logging.info(f'Processed {processed_count} trip updates.')
            session.expire_all()  # Clear session memory to prevent tracking all objects

    # Commit any remaining updates
    if new_updates:
        session.bulk_save_objects(new_updates)
        session.commit()
        logging.info(f'Processed all remaining trip updates.')

    logging.info('Trip updates stored successfully.')


def getFromCache(session, key):
    """
    Retrieves data from the cache.

    This function checks if a cache entry with the specified key exists and is still valid.
    If the cache entry is valid, it returns the cached value. Otherwise, it returns None.

    Args:
        session (Session): The database session used for querying the cache.
        key (str): The key used to identify the cache entry.

    Returns:
        bytes or None: The cached data if valid, otherwise None.
    """
    logging.info(f"Attempting to retrieve data from cache with key: {key}")
    cache_entry = session.query(Cache).filter_by(key=key).first()
    if cache_entry:
        logging.info("Cache entry found. Checking validity.")
        # Check if the cache is still valid (1 minute for GTFS-RT, 24 hours for schedule)
        if (key.startswith('gtfs_rt') and time.time() - cache_entry.timestamp < 6000) or \
           (key == 'gtfs_schedule' and time.time() - cache_entry.timestamp < 86400):
            logging.info("Cache entry is valid. Returning cached data.")
            return cache_entry.value
        else:
            logging.info("Cache entry is expired.")
    else:
        logging.info("No cache entry found.")
    return None


def saveToCache(session, key, value):
    """
    Saves data to the cache.

    This function checks if a cache entry with the specified key already exists.
    If it does, the entry is updated with the new value and timestamp.
    If it doesn't, a new cache entry is created and added to the session.

    Args:
        session (Session): The database session used for querying and storing data.
        key (str): The key used to identify the cache entry.
        value (bytes): The data to be stored in the cache.

    Returns:
        None
    """
    logging.info(f"Saving data to cache with key: {key}")
    cache_entry = session.query(Cache).filter_by(key=key).first()
    if cache_entry:
        logging.info("Updating existing cache entry.")
        cache_entry.value = value
        cache_entry.timestamp = int(time.time())
    else:
        logging.info("Creating new cache entry.")
        cache_entry = Cache(key=key, value=value, timestamp=int(time.time()))
        session.add(cache_entry)
    session.commit()
    logging.info("Cache data saved successfully.")

def fetchGtfsRtData(url, cacheKey):
    """
    Fetches GTFS Realtime data from a specified URL and caches it.

    This function first checks if the data is available in the cache. If cached data is found
    and is still valid, it returns the cached data. Otherwise, it fetches the data from the URL,
    caches it, and then returns the data.

    Args:
        url (str): The URL to fetch the GTFS Realtime data from.
        cacheKey (str): The key used to store and retrieve data from the cache.

    Returns:
        gtfs_realtime_pb2.FeedMessage: The parsed GTFS Realtime feed message.
    """
    logging.info(f"Fetching GTFS Realtime data for cacheKey: {cacheKey}")
    session = Session()
    cached_data = getFromCache(session, cacheKey)
    if cached_data:
        logging.info("Using cached GTFS Realtime data.")
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(cached_data)
        session.close()
        return feed

    logging.info("Fetching GTFS Realtime data from URL.")
    headers = {'Ocp-Apim-Subscription-Key': SUBSCRIPTION_KEY}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        logging.info("GTFS Realtime data fetched successfully. Caching data.")
        saveToCache(session, cacheKey, response.content)
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)
        session.close()
        return feed
    else:
        logging.error(f"Failed to fetch GTFS Realtime data: {response.status_code}")
        session.close()
        return None

def parseAndStoreStops(session, stops_data):
    """
    Parses and stores stop data in the database.

    This function processes the provided stop data, checks if each stop already exists
    in the database, and either updates the existing entry or creates a new one.

    Args:
        session (Session): The database session used for querying and storing data.
        stops_data (str): The CSV formatted stop data.

    Returns:
        None
    """
    logging.info('Parsing and storing stops data.')

    # Parse the data using csv.DictReader for robustness
    reader = csv.DictReader(stops_data.splitlines())

    # Pre-fetch existing stops into a dictionary for fast lookup
    logging.info('Pre-fetching existing stops...')
    existing_stops = {
        stop.stop_id: stop
        for stop in session.query(Stop).all()
    }
    logging.info(f'Loaded {len(existing_stops)} existing stops.')

    new_stops = []
    updated_stops = []

    # Process the stop data
    for index, row in enumerate(reader):
        stop_id = row['stop_id']
        stop_code = row['stop_code']
        stop_name = row['stop_name']
        tts_stop_name = row['tts_stop_name']
        stop_desc = row['stop_desc']
        stop_lat = float(row['stop_lat'])
        stop_lon = float(row['stop_lon'])
        zone_id = row['zone_id']
        stop_url = row['stop_url']
        location_type = int(row['location_type']) if row['location_type'] else None
        parent_station = row['parent_station']
        stop_timezone = row['stop_timezone']
        wheelchair_boarding = int(row['wheelchair_boarding']) if row['wheelchair_boarding'] else None
        level_id = row['level_id']
        platform_code = row['platform_code']

        # Check if the stop already exists
        if stop_id in existing_stops:
            # Update existing stop
            existing_stop = existing_stops[stop_id]
            existing_stop.stop_code = stop_code
            existing_stop.stop_name = stop_name
            existing_stop.tts_stop_name = tts_stop_name
            existing_stop.stop_desc = stop_desc
            existing_stop.stop_lat = stop_lat
            existing_stop.stop_lon = stop_lon
            existing_stop.zone_id = zone_id
            existing_stop.stop_url = stop_url
            existing_stop.location_type = location_type
            existing_stop.parent_station = parent_station
            existing_stop.stop_timezone = stop_timezone
            existing_stop.wheelchair_boarding = wheelchair_boarding
            existing_stop.level_id = level_id
            existing_stop.platform_code = platform_code
            updated_stops.append(existing_stop)
        else:
            # Create new stop entry
            new_stop = Stop(
                stop_id=stop_id,
                stop_code=stop_code,
                stop_name=stop_name,
                tts_stop_name=tts_stop_name,
                stop_desc=stop_desc,
                stop_lat=stop_lat,
                stop_lon=stop_lon,
                zone_id=zone_id,
                stop_url=stop_url,
                location_type=location_type,
                parent_station=parent_station,
                stop_timezone=stop_timezone,
                wheelchair_boarding=wheelchair_boarding,
                level_id=level_id,
                platform_code=platform_code
            )
            new_stops.append(new_stop)

    # Bulk insert and update
    if new_stops:
        session.bulk_save_objects(new_stops)
        logging.info(f'Inserted {len(new_stops)} new stops.')

    if updated_stops:
        session.bulk_update_mappings(Stop, [stop.__dict__ for stop in updated_stops])
        logging.info(f'Updated {len(updated_stops)} existing stops.')

    # Commit all changes at once
    session.commit()
    logging.info('Stops data stored successfully.')


def parseAndStoreRoutes(session, routes_data):
    """
    Parses and stores route data in the database.

    This function processes the provided route data, checks if each route already exists
    in the database, and either updates the existing entry or creates a new one.

    Args:
        session (Session): The database session used for querying and storing data.
        routes_data (str): The CSV formatted route data.

    Returns:
        None
    """
    logging.info('Parsing and storing routes data.')
    # Split the data into lines
    lines = routes_data.splitlines()
    # Extract the header to determine the column indices
    header = lines[0].split(',')
    route_id_index = header.index('route_id')
    agency_id_index = header.index('agency_id')
    route_short_name_index = header.index('route_short_name')
    route_long_name_index = header.index('route_long_name')
    route_desc_index = header.index('route_desc')
    route_type_index = header.index('route_type')
    route_url_index = header.index('route_url')
    route_color_index = header.index('route_color')
    route_text_color_index = header.index('route_text_color')

    # Pre-fetch existing routes into a dictionary for fast lookup
    existing_routes = {
        route.route_id: route
        for route in session.query(Route).all()
    }
    logging.info(f'Loaded {len(existing_routes)} existing routes.')

    new_routes = []
    updated_routes = []

    for line in lines[1:]:  # Skip the header line
        # Split each line into fields
        fields = line.split(',')
        route_id = fields[route_id_index]
        agency_id = fields[agency_id_index]
        route_short_name = fields[route_short_name_index]
        route_long_name = fields[route_long_name_index]
        route_desc = fields[route_desc_index]
        route_type = int(fields[route_type_index])
        route_url = fields[route_url_index]
        route_color = fields[route_color_index]
        route_text_color = fields[route_text_color_index]

        # Check if the route already exists
        if route_id in existing_routes:
            # Update existing route
            existing_route = existing_routes[route_id]
            existing_route.agency_id = agency_id
            existing_route.route_short_name = route_short_name
            existing_route.route_long_name = route_long_name
            existing_route.route_desc = route_desc
            existing_route.route_type = route_type
            existing_route.route_url = route_url
            existing_route.route_color = route_color
            existing_route.route_text_color = route_text_color
            updated_routes.append(existing_route)
        else:
            # Create new route entry
            new_route = Route(
                route_id=route_id,
                agency_id=agency_id,
                route_short_name=route_short_name,
                route_long_name=route_long_name,
                route_desc=route_desc,
                route_type=route_type,
                route_url=route_url,
                route_color=route_color,
                route_text_color=route_text_color
            )
            new_routes.append(new_route)

    # Bulk insert and update
    if new_routes:
        session.bulk_save_objects(new_routes)
        logging.info(f'Inserted {len(new_routes)} new routes.')

    if updated_routes:
        session.bulk_update_mappings(Route, [route.__dict__ for route in updated_routes])
        logging.info(f'Updated {len(updated_routes)} existing routes.')

    # Commit all changes at once
    session.commit()
    logging.info('Routes data stored successfully.')


def parseAndStoreShapes(session, shapes_data, batch_size=1000):
    """
    Parses and stores shape data in the database.

    This function processes the provided shape data, checks if each shape point
    already exists in the database, and either updates the existing entry or creates a new one.

    Args:
        session (Session): The database session used for querying and storing data.
        shapes_data (str): The CSV formatted shape data.
        batch_size (int): Number of records to process before committing and clearing the session.

    Returns:
        None
    """
    logging.info('Parsing and storing shapes data.')

    # Parse the data using csv.DictReader
    reader = csv.DictReader(shapes_data.splitlines())

    # Pre-fetch all existing shapes and store in a dictionary
    logging.info('Pre-fetching existing shapes...')
    existing_shapes = {
        (shape.shape_id, shape.shape_pt_sequence): shape
        for shape in session.query(Shape).all()
    }
    logging.info(f'Loaded {len(existing_shapes)} existing shapes.')

    # Initialize counters and buffers
    processed_count = 0
    new_shapes = []

    for row in reader:
        shape_id = row['shape_id']
        shape_pt_lat = float(row['shape_pt_lat'])
        shape_pt_lon = float(row['shape_pt_lon'])
        shape_pt_sequence = int(row['shape_pt_sequence'])
        shape_dist_traveled = float(row['shape_dist_traveled']) if row['shape_dist_traveled'] else None

        key = (shape_id, shape_pt_sequence)

        if key in existing_shapes:
            # Update existing shape fields
            existing_shape = existing_shapes[key]
            existing_shape.shape_pt_lat = shape_pt_lat
            existing_shape.shape_pt_lon = shape_pt_lon
            existing_shape.shape_dist_traveled = shape_dist_traveled
        else:
            # Create a new Shape entry
            new_shape = Shape(
                shape_id=shape_id,
                shape_pt_lat=shape_pt_lat,
                shape_pt_lon=shape_pt_lon,
                shape_pt_sequence=shape_pt_sequence,
                shape_dist_traveled=shape_dist_traveled
            )
            new_shapes.append(new_shape)

        # Commit in batches to reduce session size
        processed_count += 1
        if processed_count % batch_size == 0:
            if new_shapes:
                session.bulk_save_objects(new_shapes)
                new_shapes.clear()  # Clear the list to avoid memory buildup
            session.commit()
            logging.info(f'Processed {processed_count} shape points.')
            session.expire_all()  # Clear session memory to prevent tracking all objects

    # Commit any remaining new shapes
    if new_shapes:
        session.bulk_save_objects(new_shapes)
        session.commit()
        logging.info(f'Processed all remaining shapes.')

    logging.info('Shapes data stored successfully.')

def parseAndStoreCalendar(session, calendar_data):
    """
    Parses and stores calendar data in the database.

    This function processes the provided calendar data, checks if each calendar entry
    already exists in the database, and either updates the existing entry or creates a new one.

    Args:
        session (Session): The database session used for querying and storing data.
        calendar_data (str): The CSV formatted calendar data.

    Returns:
        None
    """
    logging.info('Parsing and storing calendar data.')
    # Split the data into lines
    lines = calendar_data.splitlines()
    # Extract the header to determine the column indices
    header = lines[0].split(',')
    service_id_index = header.index('service_id')
    monday_index = header.index('monday')
    tuesday_index = header.index('tuesday')
    wednesday_index = header.index('wednesday')
    thursday_index = header.index('thursday')
    friday_index = header.index('friday')
    saturday_index = header.index('saturday')
    sunday_index = header.index('sunday')
    start_date_index = header.index('start_date')
    end_date_index = header.index('end_date')

    # Pre-fetch existing calendar entries into a dictionary for fast lookup
    existing_calendars = {
        calendar.service_id: calendar
        for calendar in session.query(Calendar).all()
    }
    logging.info(f'Loaded {len(existing_calendars)} existing calendar entries.')

    new_calendars = []
    updated_calendars = []

    for line in lines[1:]:  # Skip the header line
        # Split each line into fields
        fields = line.split(',')
        service_id = fields[service_id_index]
        monday = int(fields[monday_index])
        tuesday = int(fields[tuesday_index])
        wednesday = int(fields[wednesday_index])
        thursday = int(fields[thursday_index])
        friday = int(fields[friday_index])
        saturday = int(fields[saturday_index])
        sunday = int(fields[sunday_index])
        start_date = datetime.strptime(fields[start_date_index], '%Y%m%d').date()
        end_date = datetime.strptime(fields[end_date_index], '%Y%m%d').date()

        # Check if the calendar entry already exists
        if service_id in existing_calendars:
            # Update existing calendar entry
            existing_calendar = existing_calendars[service_id]
            existing_calendar.monday = monday
            existing_calendar.tuesday = tuesday
            existing_calendar.wednesday = wednesday
            existing_calendar.thursday = thursday
            existing_calendar.friday = friday
            existing_calendar.saturday = saturday
            existing_calendar.sunday = sunday
            existing_calendar.start_date = start_date
            existing_calendar.end_date = end_date
            updated_calendars.append(existing_calendar)
        else:
            # Create new calendar entry
            new_calendar = Calendar(
                service_id=service_id,
                monday=monday,
                tuesday=tuesday,
                wednesday=wednesday,
                thursday=thursday,
                friday=friday,
                saturday=saturday,
                sunday=sunday,
                start_date=start_date,
                end_date=end_date
            )
            new_calendars.append(new_calendar)

    # Bulk insert and update
    if new_calendars:
        session.bulk_save_objects(new_calendars)
        logging.info(f'Inserted {len(new_calendars)} new calendar entries.')

    if updated_calendars:
        session.bulk_update_mappings(Calendar, [calendar.__dict__ for calendar in updated_calendars])
        logging.info(f'Updated {len(updated_calendars)} existing calendar entries.')

    # Commit all changes at once
    session.commit()
    logging.info('Calendar data stored successfully.')

def parseAndStoreCalendarDates(session, calendar_dates_data):
    """
    Parses and stores calendar dates data in the database.

    This function processes the provided calendar dates data, checks if each calendar date
    already exists in the database, and either updates the existing entry or creates a new one.

    Args:
        session (Session): The database session used for querying and storing data.
        calendar_dates_data (str): The CSV formatted calendar dates data.

    Returns:
        None
    """
    logging.info('Parsing and storing calendar dates data.')
    # Split the data into lines
    lines = calendar_dates_data.splitlines()
    # Extract the header to determine the column indices
    header = lines[0].split(',')
    service_id_index = header.index('service_id')
    date_index = header.index('date')
    exception_type_index = header.index('exception_type')

    # Pre-fetch existing calendar dates into a dictionary for fast lookup
    existing_calendar_dates = {
        (calendar_date.service_id, calendar_date.date): calendar_date
        for calendar_date in session.query(CalendarDate).all()
    }
    logging.info(f'Loaded {len(existing_calendar_dates)} existing calendar dates.')

    new_calendar_dates = []
    updated_calendar_dates = []

    for line in lines[1:]:  # Skip the header line
        # Split each line into fields
        fields = line.split(',')
        service_id = fields[service_id_index]
        date = datetime.strptime(fields[date_index], '%Y%m%d').date()
        exception_type = int(fields[exception_type_index])

        # Check if the calendar date already exists
        if (service_id, date) in existing_calendar_dates:
            # Update existing calendar date fields
            existing_calendar_date = existing_calendar_dates[(service_id, date)]
            existing_calendar_date.exception_type = exception_type
            updated_calendar_dates.append(existing_calendar_date)
        else:
            # Create new calendar date entry
            new_calendar_date = CalendarDate(
                service_id=service_id,
                date=date,
                exception_type=exception_type
            )
            new_calendar_dates.append(new_calendar_date)

    # Bulk insert and update
    if new_calendar_dates:
        session.bulk_save_objects(new_calendar_dates)
        logging.info(f'Inserted {len(new_calendar_dates)} new calendar dates.')

    if updated_calendar_dates:
        session.bulk_update_mappings(CalendarDate, [calendar_date.__dict__ for calendar_date in updated_calendar_dates])
        logging.info(f'Updated {len(updated_calendar_dates)} existing calendar dates.')

    # Commit all changes at once
    session.commit()
    logging.info('Calendar dates data stored successfully.')

def parseAndStoreTrips(session, trips_data, batch_size=1000):
    """
    Parses and stores trip data in the database.

    This function processes the provided trip data, checks if each trip already exists
    in the database, and either updates the existing entry or creates a new one.

    Args:
        session (Session): The database session used for querying and storing data.
        trips_data (str): The CSV formatted trip data.
        batch_size (int): Number of records to process before committing and clearing the session.

    Returns:
        None
    """
    logging.info('Parsing and storing trips data.')

    # Parse the data using csv.DictReader
    reader = csv.DictReader(trips_data.splitlines())

    # Pre-fetch all existing trips and store in a dictionary
    logging.info('Pre-fetching existing trips...')
    existing_trips = {
        trip.trip_id: trip
        for trip in session.query(Trip).all()
    }
    logging.info(f'Loaded {len(existing_trips)} existing trips.')

    # Initialize counters and buffers
    processed_count = 0
    new_trips = []

    for row in reader:
        trip_id = row['trip_id']
        route_id = row['route_id']
        service_id = row['service_id']
        trip_headsign = row['trip_headsign']
        trip_short_name = row['trip_short_name']
        direction_id = int(row['direction_id'])
        block_id = int(row['block_id'])
        shape_id = row['shape_id']  # Treat shape_id as a string
        wheelchair_accessible = int(row['wheelchair_accessible'])
        bikes_allowed = int(row['bikes_allowed'])

        if trip_id in existing_trips:
            # Update existing trip fields
            existing_trip = existing_trips[trip_id]
            existing_trip.route_id = route_id
            existing_trip.service_id = service_id
            existing_trip.trip_headsign = trip_headsign
            existing_trip.trip_short_name = trip_short_name
            existing_trip.direction_id = direction_id
            existing_trip.block_id = block_id
            existing_trip.shape_id = shape_id
            existing_trip.wheelchair_accessible = wheelchair_accessible
            existing_trip.bikes_allowed = bikes_allowed
        else:
            # Create a new Trip entry
            new_trip = Trip(
                trip_id=trip_id,
                route_id=route_id,
                service_id=service_id,
                trip_headsign=trip_headsign,
                trip_short_name=trip_short_name,
                direction_id=direction_id,
                block_id=block_id,
                shape_id=shape_id,
                wheelchair_accessible=wheelchair_accessible,
                bikes_allowed=bikes_allowed
            )
            new_trips.append(new_trip)

        # Commit in batches to reduce session size
        processed_count += 1
        if processed_count % batch_size == 0:
            if new_trips:
                session.bulk_save_objects(new_trips)
                new_trips.clear()  # Clear the list to avoid memory buildup
            session.commit()
            logging.info(f'Processed {processed_count} trips.')
            session.expire_all()  # Clear session memory to prevent tracking all objects

    # Commit any remaining new trips
    if new_trips:
        session.bulk_save_objects(new_trips)
        session.commit()
        logging.info(f'Processed all remaining trips.')

    logging.info('Trips data stored successfully.')


def storeStopTimeUpdates(session, trip_update_id, stop_time_updates):
    """
    Stores stop time updates in the database.

    This function iterates over the stop time updates provided, checks if each update
    already exists in the database, and either updates the existing entry or creates a new one.

    Args:
        session (Session): The database session used for querying and storing data.
        trip_update_id (int): The ID of the trip update associated with these stop time updates.
        stop_time_updates (list): A list of stop time update objects containing arrival and departure times.

    Returns:
        None
    """
    logging.info(f"Storing {len(stop_time_updates)} stop time updates for trip update ID {trip_update_id}.")
    
    # Pre-fetch existing stop time updates into a dictionary for fast lookup
    existing_stop_time_updates = {
        (update.stop_sequence, update.stop_id): update
        for update in session.query(StopTimeUpdate).filter_by(trip_update_id=trip_update_id).all()
    }
    logging.info(f"Loaded {len(existing_stop_time_updates)} existing stop time updates.")

    new_stop_time_updates = []
    updated_stop_time_updates = []

    for stop_time_update in stop_time_updates:
        # Convert arrival and departure times to datetime objects
        arrival_time = datetime.fromtimestamp(stop_time_update.arrival.time) if stop_time_update.arrival.time else None
        departure_time = datetime.fromtimestamp(stop_time_update.departure.time) if stop_time_update.departure.time else None

        # Check if the stop time update already exists
        existing_update = existing_stop_time_updates.get(
            (stop_time_update.stop_sequence, stop_time_update.stop_id)
        )

        if existing_update:
            # Update existing stop time update fields
            existing_update.arrival_time = arrival_time
            existing_update.departure_time = departure_time
            existing_update.schedule_relationship = stop_time_update.schedule_relationship
            updated_stop_time_updates.append(existing_update)
        else:
            # Create a new stop time update entry
            new_update = StopTimeUpdate(
                trip_update_id=trip_update_id,
                stop_sequence=stop_time_update.stop_sequence,
                stop_id=stop_time_update.stop_id,
                arrival_time=arrival_time,
                departure_time=departure_time,
                schedule_relationship=stop_time_update.schedule_relationship
            )
            new_stop_time_updates.append(new_update)

    # Bulk insert and bulk update
    if new_stop_time_updates:
        session.bulk_save_objects(new_stop_time_updates)
        logging.info(f'Inserted {len(new_stop_time_updates)} new stop time updates.')

    if updated_stop_time_updates:
        session.bulk_update_mappings(StopTimeUpdate, [update.__dict__ for update in updated_stop_time_updates])
        logging.info(f'Updated {len(updated_stop_time_updates)} existing stop time updates.')

    # Commit all changes at once
    session.commit()
    logging.info('Stop time updates stored successfully.')

def cleanupCache(session, max_age_seconds):
    """
    Remove cache entries older than max_age_seconds.

    This function queries the Cache table in the database and deletes entries
    that are older than the specified maximum age in seconds.

    Args:
        session (Session): The database session used for querying and deleting data.
        max_age_seconds (int): The maximum age in seconds for cache entries to be retained.

    Returns:
        None
    """
    logging.info('Cleaning up cache entries older than %d seconds.', max_age_seconds)
    current_time = int(time.time())
    deleted_count = session.query(Cache).filter(current_time - Cache.timestamp > max_age_seconds).delete()
    logging.info('Deleted %d cache entries.', deleted_count)
    session.commit()


def storeVehiclePositions(session, vehicle_positions):
    """
    Stores vehicle positions in the database.

    This function iterates over the vehicle positions provided, checks if each position
    already exists in the database, and either updates the existing entry or creates a new one.

    Args:
        session (Session): The database session used for querying and storing data.
        vehicle_positions (FeedMessage): The GTFS real-time vehicle positions data.

    Returns:
        None
    """
    logging.info(f"Storing {len(vehicle_positions.entity)} vehicle positions.")

    # Pre-fetch existing vehicle positions into a dictionary for fast lookup
    existing_positions = {
        (entity.vehicle.vehicle.id, datetime.fromtimestamp(entity.vehicle.timestamp)): entity
        for entity in session.query(VehiclePosition).all()
    }
    logging.info(f"Loaded {len(existing_positions)} existing vehicle positions.")

    new_positions = []
    updated_positions = []

    for entity in vehicle_positions.entity:
        vehicle = entity.vehicle
        # Convert the timestamp to a datetime object
        timestamp = datetime.fromtimestamp(vehicle.timestamp)

        # Check if the vehicle position already exists
        existing_position = existing_positions.get((vehicle.vehicle.id, timestamp))

        if existing_position:
            logging.info(f"Vehicle position already exists for vehicle_id: {vehicle.vehicle.id} at timestamp: {timestamp}")
        else:
            # Create a new VehiclePosition entry
            vehicle_position = VehiclePosition(
                update_id=entity.id,
                is_deleted=entity.is_deleted,
                trip_id=vehicle.trip.trip_id if vehicle.trip else None,
                route_id=vehicle.trip.route_id if vehicle.trip else None,
                vehicle_id=vehicle.vehicle.id,
                vehicle_label=vehicle.vehicle.label,
                vehicle_license_plate=vehicle.vehicle.license_plate,
                latitude=vehicle.position.latitude,
                longitude=vehicle.position.longitude,
                bearing=vehicle.position.bearing,
                odometer=vehicle.position.odometer,
                speed=vehicle.position.speed,
                timestamp=timestamp,
                congestion_level=vehicle.congestion_level,
                occupancy_status=vehicle.occupancy_status,
                occupancy_percentage=vehicle.occupancy_percentage,
                current_stop_sequence=vehicle.current_stop_sequence,
                stop_id=vehicle.stop_id,
                current_status=vehicle.current_status
            )
            new_positions.append(vehicle_position)

    # Bulk insert new positions
    if new_positions:
        session.bulk_save_objects(new_positions)
        logging.info(f"Inserted {len(new_positions)} new vehicle positions.")

    # Commit all changes at once
    session.commit()
    logging.info('Vehicle positions stored successfully.')


def fetchGtfsRtVehiclePositions():
    """
    Fetches GTFS real-time vehicle positions from a specified URL, caches the data,
    and stores the positions in the database.

    The function first checks if the vehicle positions data is already cached. If cached data
    is found, it uses the cached data. Otherwise, it fetches the data from the URL,
    caches the response, and stores the vehicle positions in the database.

    Returns:
        FeedMessage: The GTFS real-time vehicle positions if fetched successfully, or the cached
                     data if available. Returns None if the fetch operation fails.
    """
    url = 'https://nextrip-public-api.azure-api.net/octranspo/gtfs-rt-vp/beta/v1/VehiclePositions'
    cacheKey = 'gtfs_rt_vehicle_positions'
    session = Session()
    logging.info('Fetching GTFS real-time vehicle positions.')
    vehicle_positions = fetchGtfsRtData(url, cacheKey)
    if vehicle_positions:
        logging.info('Storing vehicle positions in the database.')
        storeVehiclePositions(session, vehicle_positions)
    else:
        logging.warning('Failed to fetch or store vehicle positions.')
    session.close()
    return vehicle_positions


def fetchGtfsRtTripUpdates():
    """
    Fetches GTFS real-time trip updates from a specified URL, caches the data,
    and stores the updates in the database.

    The function first checks if the trip updates data is already cached. If cached data
    is found, it uses the cached data. Otherwise, it fetches the data from the URL,
    caches the response, and stores the trip updates in the database.

    Returns:
        FeedMessage: The GTFS real-time trip updates if fetched successfully, or the cached
                     data if available. Returns None if the fetch operation fails.
    """
    url = 'https://nextrip-public-api.azure-api.net/octranspo/gtfs-rt-tp/beta/v1/TripUpdates'
    cacheKey = 'gtfs_rt_trip_updates'
    session = Session()
    logging.info('Fetching GTFS real-time trip updates.')
    trip_updates = fetchGtfsRtData(url, cacheKey)
    if trip_updates:
        logging.info('Storing trip updates in the database.')
        storeTripUpdates(session, trip_updates)
    else:
        logging.warning('Failed to fetch or store trip updates.')
    session.close()
    return trip_updates


def fetchGtfsSchedule():
    """
    Fetches the GTFS schedule data from a specified URL, caches it, and processes
    various files within the downloaded zip to store data in a database.

    The function first checks if the schedule data is already cached. If cached data
    is found, it returns the cached data. Otherwise, it fetches the data from the URL,
    caches the response, and processes each file in the zip archive to store the data
    in the database.

    Returns:
        bytes: The content of the GTFS schedule if fetched successfully, or the cached
               data if available. Returns None if the fetch operation fails.
    """
    session = Session()
    cleanupCache(session, 60 * 60 * 24)
    cached_data = getFromCache(session, 'gtfs_schedule')
    if cached_data:
        logging.info('Using cached GTFS schedule data.')
        with zipfile.ZipFile(io.BytesIO(cached_data)) as z:
            for filename in z.namelist():
                with z.open(filename) as f:
                    file_data = f.read().decode('utf-8')
                    logging.info(f'Processing file: {filename}')
                    if filename == 'stops.txt':
                        parseAndStoreStops(session, file_data)
                    elif filename == 'trips.txt':
                        parseAndStoreTrips(session, file_data)
                    elif filename == 'shapes.txt':
                        parseAndStoreShapes(session, file_data)
                    elif filename == 'routes.txt':
                        parseAndStoreRoutes(session, file_data)
                    elif filename == 'calendar_dates.txt':
                        parseAndStoreCalendarDates(session, file_data)
                    elif filename == 'calendar.txt':
                        parseAndStoreCalendar(session, file_data)
                    elif filename == 'stop_times.txt':
                        parseAndStoreStopTimes(session, file_data)

        session.close()
        return cached_data

    url = 'https://oct-gtfs-emasagcnfmcgeham.z01.azurefd.net/public-access/GTFSExport.zip'
    response = requests.get(url)
    if response.status_code == 200:
        logging.info('Fetched GTFS schedule data successfully.')
        saveToCache(session, 'gtfs_schedule', response.content)
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            for filename in z.namelist():
                with z.open(filename) as f:
                    file_data = f.read().decode('utf-8')
                    logging.info(f'Processing file: {filename}')
                    if filename == 'stops.txt':
                        parseAndStoreStops(session, file_data)
                    elif filename == 'trips.txt':
                        parseAndStoreTrips(session, file_data)
                    elif filename == 'shapes.txt':
                        parseAndStoreShapes(session, file_data)
                    elif filename == 'routes.txt':
                        parseAndStoreRoutes(session, file_data)
                    elif filename == 'calendar_dates.txt':
                        parseAndStoreCalendarDates(session, file_data)
                    elif filename == 'calendar.txt':
                        parseAndStoreCalendar(session, file_data)
                    elif filename == 'stop_times.txt':
                        parseAndStoreStopTimes(session, file_data)
        session.close()
        return response.content
    else:
        logging.error('Failed to fetch GTFS schedule: %s', response.status_code)
        session.close()
        return None
