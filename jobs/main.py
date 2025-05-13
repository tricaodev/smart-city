import json
import os
import random
import time
import uuid

from confluent_kafka import SerializingProducer
from datetime import datetime, timezone, timedelta

LONDON_COORDINATES = {'latitude': 51.4893335, 'longitude': -0.1440551}
BIRMINGHAM_COORDINATES = {'latitude': 52.4796992, 'longitude': -1.9026911}

LATITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['latitude'] - LONDON_COORDINATES['latitude']) / 100
LONGITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['longitude'] - LONDON_COORDINATES['longitude']) / 100

KAFKA_BROKER = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")

VEHICLE_TOPIC = os.getenv("vehicle_topic", "vehicle_data")
GPS_TOPIC = os.getenv("gps_topic", "gps_data")
CAMERA_TOPIC = os.getenv("camera_topic", "camera_data")
WEATHER_TOPIC = os.getenv("weather_topic", "weather_data")
EMERGENCY_TOPIC = os.getenv("emergency_topic", "emergency_data")

start_time = datetime.now(timezone(timedelta(hours=7)))
start_location = LONDON_COORDINATES.copy()

random.seed(42)

def simulate_vehicle_movement():
    global start_location

    start_location['latitude'] += LATITUDE_INCREMENT + random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += LONGITUDE_INCREMENT + random.uniform(-0.0005, 0.0005)

    return start_location

def get_next_time():
    global start_time

    start_time += timedelta(seconds=random.randint(30, 60))

    return start_time

def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()

    return {
        "id": uuid.uuid4(),
        "deviceId": device_id,
        "timestamp": get_next_time().isoformat(),
        "location": (location['latitude'], location['longitude']),
        "speed": random.uniform(10, 40),
        "direction": "North-East",
        "make": "BMW",
        "model": "C500",
        "year": 2025,
        "fuelType": "Hybrid"
    }

def generate_gps_data(device_id, location, timestamp, vehicle_type="private"):
    return {
        "id": uuid.uuid4(),
        "deviceId": device_id,
        "timestamp": timestamp,
        "location": location,
        "speed": random.uniform(0, 40),
        "direction": "North-East",
        "vehicle_type": vehicle_type
    }

def generate_camera_data(device_id, timestamp, location, camera_id):
    return {
        "id": uuid.uuid4(),
        "deviceId": device_id,
        "cameraId": camera_id,
        "location": location,
        "timestamp": timestamp,
        "snapshot": "Base64EncodedString"
    }

def generate_weather_data(device_id, location, timestamp):
    return {
        "id": uuid.uuid4(),
        "deviceId": device_id,
        "location": location,
        "timestamp": timestamp,
        "temperature": random.uniform(-5, 26),
        "weatherCondition": random.choice(["Sunny", "Cloudy", "Rain", "Snow"]),
        "precipitation": random.uniform(0, 25),
        "windSpeed": random.uniform(0, 100),
        "humidity": random.randint(0, 100),
        "airQualityIndex": random.uniform(0, 500)
    }

def generate_emergency_data(device_id, timestamp, location):
    return {
        "id": uuid.uuid4(),
        "deviceId": device_id,
        "incidentId": uuid.uuid4(),
        "type": random.choice(["Accident", "Fire", "Medical", "Police", "None"]),
        "timestamp": timestamp,
        "location": location,
        "status": random.choice(["Active", "Resolved"]),
        "description": "Description of the incident"
    }

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def json_default(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f"Type {type(obj)} not serializable")

def produce_data_to_kafka_topic(producer, data, topic):
    producer.produce(
        topic=topic,
        key=str(data["id"]),
        value=json.dumps(data, default=json_default),
        on_delivery=delivery_report
    )

def simulate_journey(producer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data["location"], vehicle_data["timestamp"])
        camera_data = generate_camera_data(device_id, vehicle_data["timestamp"], vehicle_data["location"], "Nikon-Camera161")
        weather_data = generate_weather_data(device_id, vehicle_data["location"], vehicle_data["timestamp"])
        emergency_data = generate_emergency_data(device_id, vehicle_data["timestamp"], vehicle_data["location"])

        if start_location['latitude'] > BIRMINGHAM_COORDINATES['latitude'] and start_location['longitude'] < BIRMINGHAM_COORDINATES['longitude']:
            print("Vehicle has reached Birmingham. Simulation ending...")
            break

        produce_data_to_kafka_topic(producer, vehicle_data, VEHICLE_TOPIC)
        produce_data_to_kafka_topic(producer, gps_data, GPS_TOPIC)
        produce_data_to_kafka_topic(producer, camera_data, CAMERA_TOPIC)
        produce_data_to_kafka_topic(producer, weather_data, WEATHER_TOPIC)
        produce_data_to_kafka_topic(producer, emergency_data, EMERGENCY_TOPIC)

        producer.flush()
        time.sleep(5)

if __name__ == "__main__":
    producer = SerializingProducer({
        'bootstrap.servers': KAFKA_BROKER,
        'error_cb': lambda err: print(f"Kafka error: {err}")
    })

    try:
        simulate_journey(producer, "Vehicle-TriCao-161")

    except KeyboardInterrupt:
        print("Simulation ended by the user!")

    except Exception as e:
        print(f"An error has occurred: {e}")