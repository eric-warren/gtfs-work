from getGtfs import fetchGtfsRtVehiclePositions, fetchGtfsSchedule, fetchGtfsRtTripUpdates
import logging

logging.basicConfig(level=logging.INFO)
logging.basicConfig(format="%(levelname)s:%(name)s:%(message)s")


def main():

    # Fetch GTFS real-time vehicle positions
    vehicle_positions = fetchGtfsRtVehiclePositions()
    if vehicle_positions:
        print("Fetched GTFS real-time vehicle positions.")

    # Fetch GTFS real-time trip updates
    trip_updates = fetchGtfsRtTripUpdates()
    if trip_updates:
        print("Fetched GTFS real-time trip updates.")

    # Fetch GTFS schedule data
    schedule_data = fetchGtfsSchedule()
    if schedule_data:
        print("Fetched GTFS schedule data.")

if __name__ == "__main__":
    main()
