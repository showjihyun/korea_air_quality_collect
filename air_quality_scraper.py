import requests
import psycopg2
from psycopg2 import Error
import json
from urllib.parse import quote
from datetime import datetime
import logging
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('air_quality.log'),
        logging.StreamHandler()
    ]
)

# API Configuration from environment variables
측정소_API_URL = os.getenv('측정소_API_URL')
실시간_측정소_정보_API_URL = os.getenv('실시간_측정소_정보_API_URL')
API_KEY = os.getenv('API_KEY')

# Database Configuration from environment variables
DATABASE_URL = os.getenv('DATABASE_URL')

def fetch_air_measurement_data():
    # Construct GET URL with parameters
    url = f"{측정소_API_URL}?serviceKey={API_KEY}&returnType=json&numOfRows=9999&pageNo=1"
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad status codes
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching measurement station data: {e}")
        return None

def fetch_real_time_air_measurement_data(station_name):
    if station_name == None:
        logging.error("Station name is None")
        return None
    # Construct GET URL with parameters
    url = f"{실시간_측정소_정보_API_URL}?serviceKey={API_KEY}&returnType=json&numOfRows=100&pageNo=1&stationName={quote(station_name)}&dataTerm=DAILY"
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad status codes
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching real-time air quality data: {e}")
        return None

def insert_data(conn, data):
    try:
        cursor = conn.cursor()
        today = datetime.now().strftime('%Y%m%d')
        
        # Check if data for today already exists
        check_query = """
        SELECT COUNT(*) FROM measuring_station WHERE std_dt = %s;
        """
        cursor.execute(check_query, (today,))
        count = cursor.fetchone()[0]
        
        if count > 0:
            logging.info(f"Data for {today} already exists. Skipping insert.")
            return
            
        insert_query = """
        INSERT INTO measuring_station 
        (std_dt, stationname, addr, year, mangname, item, dmx, dmy)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """
        
        # Prepare data for bulk insert
        values = []
        for item in data['response']['body']['items']:
            # Handle None values with appropriate defaults
            station_name = item.get('stationName') if item.get('stationName') is not None else 'Unknown'
            addr = item.get('addr') if item.get('addr') is not None else 'Unknown'
            year = int(item.get('year')) if item.get('year') is not None else 0
            mang_name = item.get('mangName') if item.get('mangName') is not None else 'Unknown'
            item_str = item.get('item') if item.get('item') is not None else 'Unknown'
            dmx = float(item.get('dmX')) if item.get('dmX') is not None else 0.0
            dmy = float(item.get('dmY')) if item.get('dmY') is not None else 0.0
            
            values.append((
                today,
                station_name,
                addr,
                year,
                mang_name,
                item_str,
                dmx,
                dmy
            ))
        
        # Execute bulk insert
        cursor.executemany(insert_query, values)
        conn.commit()
        logging.info(f"Successfully inserted {len(values)} records for {today}")
    except Error as e:
        logging.error(f"Error inserting data: {e}")

def delete_old_data(conn):
    try:
        cursor = conn.cursor()
        delete_query = """
        DELETE FROM measuring_station 
        WHERE TO_DATE(std_dt::text, 'YYYYMMDD') < CURRENT_DATE - INTERVAL '7 days';
        """
        cursor.execute(delete_query)
        deleted_count = cursor.rowcount
        conn.commit()
        logging.info(f"Deleted {deleted_count} records older than 7 days")
    except Error as e:
        logging.error(f"Error deleting old data: {e}")
        conn.rollback()  # Rollback on error

def delete_old_realtime_data(conn):
    try:
        cursor = conn.cursor()
        delete_query = """
        DELETE FROM measuring_station_realtime 
        WHERE TO_DATE(std_dt::text, 'YYYYMMDD') < CURRENT_DATE - INTERVAL '7 days';
        """
        cursor.execute(delete_query)
        deleted_count = cursor.rowcount
        conn.commit()
        logging.info(f"Deleted {deleted_count} records older than 7 days from measuring_station_realtime")
    except Error as e:
        logging.error(f"Error deleting old realtime data: {e}")
        conn.rollback()  # Rollback on error        

def get_today_stations(conn):
    try:
        cursor = conn.cursor()
        today = datetime.now().strftime('%Y%m%d')
        query = """
        SELECT stationname FROM measuring_station 
        WHERE std_dt = %s;
        """
        cursor.execute(query, (today,))
        stations = [row[0] for row in cursor.fetchall()]
        logging.info(f"Found {len(stations)} stations for {today}")
        return stations
    except Error as e:
        logging.error(f"Error fetching stations: {e}")
        return []

def insert_realtime_data(conn, station_name, data, today):
    try:
        cursor = conn.cursor()
        
        insert_query = """
        INSERT INTO measuring_station_realtime 
        (std_dt, station_name, data_Time, so2value, so2grade, covalue, cograde, 
         o3value, o3grade, no2value, no2grade, pm10value, pm10grade, 
         pm25value, pm25grade, khaivalue, khaigrade)
        VALUES (%s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        
        # Get the most recent data based on dataTime
        items = data['response']['body']['items']
        if not items:
            logging.info(f"No data available for station {station_name}")
            return
            
        # Sort items by dataTime in descending order and get the most recent one
        latest_item = max(items, key=lambda x: x.get('dataTime', ''))
        
        # Handle None values and '-' by converting them to 0
        so2_value = 0 if latest_item.get('so2Value') is None or latest_item.get('so2Value') == '-' else float(latest_item.get('so2Value'))
        so2_grade = 0 if latest_item.get('so2Grade') is None else int(latest_item.get('so2Grade'))
        co_value = 0 if latest_item.get('coValue') is None or latest_item.get('coValue') == '-' else float(latest_item.get('coValue'))
        co_grade = 0 if latest_item.get('coGrade') is None else int(latest_item.get('coGrade'))
        o3_value = 0 if latest_item.get('o3Value') is None or latest_item.get('o3Value') == '-' else float(latest_item.get('o3Value'))
        o3_grade = 0 if latest_item.get('o3Grade') is None else int(latest_item.get('o3Grade'))
        no2_value = 0 if latest_item.get('no2Value') is None or latest_item.get('no2Value') == '-' else float(latest_item.get('no2Value'))
        no2_grade = 0 if latest_item.get('no2Grade') is None else int(latest_item.get('no2Grade'))
        pm10_value = 0 if latest_item.get('pm10Value') is None or latest_item.get('pm10Value') == '-' else int(latest_item.get('pm10Value'))
        pm10_grade = 0 if latest_item.get('pm10Grade') is None else int(latest_item.get('pm10Grade'))
        pm25_value = 0 if latest_item.get('pm25Value') is None or latest_item.get('pm25Value') == '-' else int(latest_item.get('pm25Value'))
        pm25_grade = 0 if latest_item.get('pm25Grade') is None else int(latest_item.get('pm25Grade'))
        khai_value = 0 if latest_item.get('khaiValue') is None or latest_item.get('khaiValue') == '-' else int(latest_item.get('khaiValue'))
        khai_grade = 0 if latest_item.get('khaiGrade') is None else int(latest_item.get('khaiGrade'))
        data_time = latest_item.get('dataTime')
        
        values = [(
            today,
            station_name,
            data_time,
            so2_value,
            so2_grade,
            co_value,
            co_grade,
            o3_value,
            o3_grade,
            no2_value,
            no2_grade,
            pm10_value,
            pm10_grade,
            pm25_value,
            pm25_grade,
            khai_value,
            khai_grade
        )]
        
        # Execute bulk insert
        cursor.executemany(insert_query, values)
        conn.commit()
        logging.info(f"Successfully inserted latest data for station {station_name} at {data_time}")
    except Error as e:
        logging.error(f"Error inserting realtime data for {station_name}: {e}")
        conn.rollback()

def main():
    logging.info("Starting air quality data collection")
    # Fetch measurement station data
    station_data = fetch_air_measurement_data()
    if not station_data:
        logging.error("Failed to fetch measurement station data")
        return

    try:
        # Connect to PostgreSQL using DATABASE_URL
        conn = psycopg2.connect(DATABASE_URL)
        logging.info("Successfully connected to database")
        
        # Delete old data from both tables
        delete_old_data(conn)
        delete_old_realtime_data(conn)
        
        # Insert measurement station data
        insert_data(conn, station_data)
        
        # Get today's stations
        stations = get_today_stations(conn)
        
        # Fetch and insert real-time data for each station
        today = datetime.now().strftime('%Y%m%d')
        for station in stations:
            logging.info(f"Fetching real-time data for station: {station}")
            realtime_data = fetch_real_time_air_measurement_data(station)
            if realtime_data:
                insert_realtime_data(conn, station, realtime_data, today)
            else:
                logging.error(f"Failed to fetch real-time data for station: {station}")
        
    except Error as e:
        logging.error(f"Error connecting to PostgreSQL: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed")

if __name__ == "__main__":
    main() 