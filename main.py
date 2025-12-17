import sys
import requests
import json
import pytz
from datetime import datetime
import argparse
import time
import sqlite3
from dataclasses import dataclass, asdict
from typing import Dict, Optional, List, Tuple
import os
from pathlib import Path
import math
from enum import Enum
import hashlib
import urllib3

# Disable SSL warnings for clean output
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class DataSource(Enum):
    CACHE = "cache"
    API = "api"
    FALLBACK = "fallback"

@dataclass
class TemporalData:
    city: str
    time_str: str
    timestamp: float
    source: DataSource

@dataclass
class AtmosphericData:
    city: str
    temperature: float
    condition: str
    humidity: int
    wind_speed: float
    timestamp: float
    source: DataSource

@dataclass
class CityConfig:
    timezone: str
    display_name: str
    coordinates: Tuple[float, float]
    weather_api_id: Optional[str] = None

class ResourceManager:
    def __init__(self):
        self.base_path = Path.home() / ".worldmatrix"
        self.base_path.mkdir(exist_ok=True, parents=True)
        
        self.time_db = self.base_path / "temporal.db"
        self.weather_db = self.base_path / "atmospheric.db"
        self.config_file = self.base_path / "config.json"
        
        self.init_temporal_db()
        self.init_atmospheric_db()
        self.load_configuration()
    
    def load_configuration(self):
        default_config = {
            "openweather_api_key": os.getenv("OPENWEATHER_API_KEY", ""),
            "worldtimeapi_key": os.getenv("WORLDTIMEAPI_KEY", ""),
            "refresh_interval": 300,
            "units": "metric",
            "cache_ttl": 600
        }
        
        if self.config_file.exists():
            try:
                with open(self.config_file) as f:
                    self.config = {**default_config, **json.load(f)}
            except json.JSONDecodeError:
                self.config = default_config
        else:
            self.config = default_config
            self.save_configuration()
    
    def save_configuration(self):
        with open(self.config_file, 'w') as f:
            json.dump(self.config, f, indent=2)
    
    def init_temporal_db(self):
        with sqlite3.connect(self.time_db) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS temporal_data (
                    city TEXT PRIMARY KEY,
                    time_str TEXT,
                    timestamp REAL,
                    source TEXT,
                    hash TEXT
                )
            """)
    
    def init_atmospheric_db(self):
        with sqlite3.connect(self.weather_db) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS atmospheric_data (
                    city TEXT PRIMARY KEY,
                    temperature REAL,
                    condition TEXT,
                    humidity INTEGER,
                    wind_speed REAL,
                    timestamp REAL,
                    source TEXT,
                    hash TEXT
                )
            """)
    
    def store_temporal(self, data: TemporalData):
        data_hash = hashlib.md5(f"{data.time_str}{data.timestamp}".encode()).hexdigest()
        with sqlite3.connect(self.time_db) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO temporal_data VALUES (?, ?, ?, ?, ?)
            """, (data.city, data.time_str, data.timestamp, data.source.value, data_hash))
    
    def retrieve_temporal(self, city: str, ttl: int) -> Optional[TemporalData]:
        cutoff = time.time() - ttl
        with sqlite3.connect(self.time_db) as conn:
            row = conn.execute("""
                SELECT * FROM temporal_data WHERE city = ? AND timestamp > ?
            """, (city, cutoff)).fetchone()
        
        if row:
            return TemporalData(
                city=row[0],
                time_str=row[1],
                timestamp=row[2],
                source=DataSource(row[3])
            )
        return None
    
    def store_atmospheric(self, data: AtmosphericData):
        data_hash = hashlib.md5(f"{data.temperature}{data.condition}{data.timestamp}".encode()).hexdigest()
        with sqlite3.connect(self.weather_db) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO atmospheric_data VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (data.city, data.temperature, data.condition, data.humidity, 
                  data.wind_speed, data.timestamp, data.source.value, data_hash))
    
    def retrieve_atmospheric(self, city: str, ttl: int) -> Optional[AtmosphericData]:
        cutoff = time.time() - ttl
        with sqlite3.connect(self.weather_db) as conn:
            row = conn.execute("""
                SELECT * FROM atmospheric_data WHERE city = ? AND timestamp > ?
            """, (city, cutoff)).fetchone()
        
        if row:
            return AtmosphericData(
                city=row[0],
                temperature=row[1],
                condition=row[2],
                humidity=row[3],
                wind_speed=row[4],
                timestamp=row[5],
                source=DataSource(row[6])
            )
        return None

class TemporalAcquisition:
    CITIES = {
        "london": CityConfig(
            timezone="Europe/London",
            display_name="London",
            coordinates=(51.5074, -0.1278),
            weather_api_id="2643743"
        ),
        "tokyo": CityConfig(
            timezone="Asia/Tokyo",
            display_name="Tokyo",
            coordinates=(35.6762, 139.6503),
            weather_api_id="1850147"
        ),
        "newyork": CityConfig(
            timezone="America/New_York",
            display_name="New York",
            coordinates=(40.7128, -74.0060),
            weather_api_id="5128581"
        )
    }
    
    def __init__(self, resource_mgr: ResourceManager):
        self.resource_mgr = resource_mgr
        self.session = requests.Session()
        self.session.timeout = 5
    
    def acquire_temporal(self, city_id: str) -> TemporalData:
        config = self.CITIES[city_id]
        cached = self.resource_mgr.retrieve_temporal(city_id, 
                    self.resource_mgr.config["cache_ttl"])
        if cached:
            return cached
        
        api_time = self._acquire_from_worldtimeapi(city_id, config)
        if api_time:
            self.resource_mgr.store_temporal(api_time)
            return api_time
        
        fallback_time = self._acquire_fallback(config)
        fallback_time.city = city_id  # Ensure correct city ID
        self.resource_mgr.store_temporal(fallback_time)
        return fallback_time
    
    def _acquire_from_worldtimeapi(self, city_id: str, config: CityConfig) -> Optional[TemporalData]:
        try:
            api_key = self.resource_mgr.config["worldtimeapi_key"]
            base_url = "http://worldtimeapi.org/api/timezone"
            url = f"{base_url}/{config.timezone}"
            if api_key:
                url += f"?key={api_key}"
            
            response = self.session.get(url, timeout=3)
            if response.status_code == 200:
                data = response.json()
                dt_str = data['datetime'].replace('Z', '+00:00')
                dt = datetime.fromisoformat(dt_str)
                tz = pytz.timezone(config.timezone)
                localized = dt.astimezone(tz)
                
                return TemporalData(
                    city=city_id,
                    time_str=localized.strftime("%Y-%m-%d %H:%M:%S %Z"),
                    timestamp=time.time(),
                    source=DataSource.API
                )
        except Exception as e:
            # Silent fail - use fallback
            pass
        return None
    
    def _acquire_fallback(self, config: CityConfig) -> TemporalData:
        tz = pytz.timezone(config.timezone)
        now = datetime.now(tz)
        
        return TemporalData(
            city=config.timezone.split('/')[-1].lower().replace('_', ''),
            time_str=now.strftime("%Y-%m-%d %H:%M:%S %Z%z"),
            timestamp=time.time(),
            source=DataSource.FALLBACK
        )

class AtmosphericAcquisition:
    def __init__(self, resource_mgr: ResourceManager):
        self.resource_mgr = resource_mgr
        self.session = requests.Session()
        self.session.timeout = 5
    
    def acquire_atmospheric(self, city_id: str) -> AtmosphericData:
        cached = self.resource_mgr.retrieve_atmospheric(city_id, 
                    self.resource_mgr.config["cache_ttl"])
        if cached:
            return cached
        
        config = TemporalAcquisition.CITIES[city_id]
        api_key = self.resource_mgr.config["openweather_api_key"]
        
        if not api_key or api_key.strip() == "":
            return self._generate_fallback_data(city_id, config)
        
        weather_data = self._acquire_from_openweather(config, api_key)
        if weather_data:
            self.resource_mgr.store_atmospheric(weather_data)
            return weather_data
        
        fallback = self._generate_fallback_data(city_id, config)
        self.resource_mgr.store_atmospheric(fallback)
        return fallback
    
    def _acquire_from_openweather(self, config: CityConfig, api_key: str) -> Optional[AtmosphericData]:
        try:
            base_url = "https://api.openweathermap.org/data/2.5/weather"
            units = self.resource_mgr.config.get('units', 'metric')
            
            # Use coordinates if no city ID
            if config.weather_api_id:
                params = {
                    'id': config.weather_api_id,
                    'appid': api_key,
                    'units': units
                }
            else:
                params = {
                    'lat': config.coordinates[0],
                    'lon': config.coordinates[1],
                    'appid': api_key,
                    'units': units
                }
            
            response = self.session.get(base_url, params=params, timeout=3)
            if response.status_code == 200:
                data = response.json()
                
                return AtmosphericData(
                    city=config.display_name,
                    temperature=data['main']['temp'],
                    condition=data['weather'][0]['description'].title(),
                    humidity=data['main']['humidity'],
                    wind_speed=data['wind']['speed'],
                    timestamp=time.time(),
                    source=DataSource.API
                )
        except Exception as e:
            # Fallback to generated data
            pass
        return None
    
    def _generate_fallback_data(self, city_id: str, config: CityConfig) -> AtmosphericData:
        tz = pytz.timezone(config.timezone)
        now = datetime.now(tz)
        month = now.month
        hour = now.hour
        
        # Base temperatures by city
        base_temps = {
            "london": 10,
            "tokyo": 16,
            "newyork": 12
        }
        
        base_temp = base_temps.get(city_id, 15)
        
        # Seasonal variation
        temp_variation = math.sin((month - 1) * math.pi / 6) * 8
        # Daily variation
        hour_variation = math.sin((hour - 12) * math.pi / 12) * 3
        
        temperature = base_temp + temp_variation + hour_variation
        
        # Conditions based on month and hour
        if month in [12, 1, 2]:
            conditions = ["Clear", "Partly Cloudy", "Cloudy", "Light Snow"]
        elif month in [6, 7, 8]:
            conditions = ["Clear", "Partly Cloudy", "Cloudy", "Light Rain"]
        else:
            conditions = ["Clear", "Partly Cloudy", "Cloudy", "Light Rain"]
        
        condition = conditions[(month + hour) % 4]
        
        # Convert to imperial if configured
        units = self.resource_mgr.config.get("units", "metric")
        if units == "imperial":
            temperature = (temperature * 9/5) + 32
            wind_speed = 3.5 + (month % 3) * 0.621371  # Convert to mph
        else:
            wind_speed = 3.5 + (month % 3)
        
        return AtmosphericData(
            city=config.display_name,
            temperature=round(temperature, 1),
            condition=condition,
            humidity=65 + (month * 2) % 20,
            wind_speed=round(wind_speed, 1),
            timestamp=time.time(),
            source=DataSource.FALLBACK
        )

class DisplayEngine:
    @staticmethod
    def generate_city_matrix(temporal: TemporalData, atmospheric: AtmosphericData, 
                           config: CityConfig) -> str:
        time_parts = temporal.time_str.split()
        time_display = f"{time_parts[1]} {time_parts[2] if len(time_parts) > 2 else ''}"
        
        units = os.getenv('UNITS', 'metric')
        if units == 'metric':
            temp_unit = "°C"
            wind_unit = "m/s"
        else:
            temp_unit = "°F"
            wind_unit = "mph"
        
        weather_display = f"{atmospheric.temperature:.1f}{temp_unit} | {atmospheric.condition}"
        humidity_display = f"Humidity: {atmospheric.humidity}%"
        wind_display = f"Wind: {atmospheric.wind_speed:.1f}{wind_unit}"
        
        source_symbol = {
            DataSource.CACHE: "⚡",
            DataSource.API: "📡",
            DataSource.FALLBACK: "🔄"
        }
        
        symbol = source_symbol.get(temporal.source, "❓")
        source_display = f"{symbol} {temporal.source.value}"
        
        width = 42
        border = "═" * (width - 2)
        
        lines = []
        lines.append(f"╔{border}╗")
        lines.append(f"║ {config.display_name:38} ║")
        lines.append(f"╠{border}╣")
        lines.append(f"║ Time:    {time_display:28} ║")
        lines.append(f"║ Weather: {weather_display:28} ║")
        lines.append(f"║          {humidity_display:28} ║")
        lines.append(f"║          {wind_display:28} ║")
        lines.append(f"║          {'-'*28} ║")
        lines.append(f"║ Zone:    {config.timezone:28} ║")
        lines.append(f"║ Source:  {source_display:28} ║")
        lines.append(f"╚{border}╝")
        
        return "\n".join(lines)
    
    @staticmethod
    def generate_comparative_matrix(city_data: Dict[str, Tuple[TemporalData, AtmosphericData]]):
        headers = ["City", "Time", "Temp", "Condition", "Humidity", "Wind", "Source"]
        rows = []
        
        units = os.getenv('UNITS', 'metric')
        temp_unit = "°C" if units == 'metric' else "°F"
        wind_unit = "m/s" if units == 'metric' else "mph"
        
        for city_id, (temp, atmos) in city_data.items():
            config = TemporalAcquisition.CITIES[city_id]
            time_parts = temp.time_str.split()
            time_short = f"{time_parts[1]}"
            if len(time_parts) > 2:
                tz_short = time_parts[2][:3] if len(time_parts[2]) > 2 else time_parts[2]
                time_short += f" {tz_short}"
            
            source_symbol = {
                DataSource.CACHE: "⚡",
                DataSource.API: "📡",
                DataSource.FALLBACK: "🔄"
            }.get(temp.source, "❓")
            
            condition_short = atmos.condition[:10] if len(atmos.condition) > 10 else atmos.condition
            
            rows.append([
                config.display_name,
                time_short,
                f"{atmos.temperature:.1f}{temp_unit}",
                condition_short,
                f"{atmos.humidity}%",
                f"{atmos.wind_speed:.1f}{wind_unit}",
                source_symbol
            ])
        
        col_widths = [max(len(str(row[i])) for row in [headers] + rows) + 2 
                     for i in range(len(headers))]
        
        def create_row(items, widths):
            return "│" + "│".join(f" {item:<{width-2}} " for item, width in zip(items, widths)) + "│"
        
        matrix = []
        matrix.append("┌" + "┬".join("─" * w for w in col_widths) + "┐")
        matrix.append(create_row(headers, col_widths))
        matrix.append("├" + "┼".join("─" * w for w in col_widths) + "┤")
        for row in rows:
            matrix.append(create_row(row, col_widths))
        matrix.append("└" + "┴".join("─" * w for w in col_widths) + "┘")
        
        return "\n".join(matrix)

class CommandInterface:
    def __init__(self):
        self.resource_mgr = ResourceManager()
        self.temporal_engine = TemporalAcquisition(self.resource_mgr)
        self.atmospheric_engine = AtmosphericAcquisition(self.resource_mgr)
        self.display_engine = DisplayEngine()
        self.setup_parser()
    
    def setup_parser(self):
        self.parser = argparse.ArgumentParser(
            prog="matrix",
            description="Temporal-Atmospheric Surveillance System",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Operational Protocols:
┌────────────────────┬─────────────────────────────────────────────┐
│ Directive          │ Function                                    │
├────────────────────┼─────────────────────────────────────────────┤
│ matrix             │ Full temporal-atmospheric display           │
│ matrix --city X    │ Isolate city X (london|tokyo|newyork)      │
│ matrix --watch     │ Continuous surveillance mode                │
│ matrix --compare   │ Comparative analysis matrix                 │
│ matrix --raw       │ Unformatted data stream (JSON)              │
│ matrix --config    │ System configuration interface              │
│ matrix --set-key X │ Configure API key X=openweather|worldtime   │
│ matrix --units X   │ Set units (metric|imperial)                 │
└────────────────────┴─────────────────────────────────────────────┘
            """
        )
        
        self.parser.add_argument(
            "--city",
            choices=["london", "tokyo", "newyork", "all"],
            default="all",
            metavar="ZONE",
            help="Target city specification"
        )
        
        self.parser.add_argument(
            "--watch",
            action="store_true",
            help="Activate continuous surveillance"
        )
        
        self.parser.add_argument(
            "--compare",
            action="store_true",
            help="Generate comparative analysis matrix"
        )
        
        self.parser.add_argument(
            "--raw",
            action="store_true",
            help="Output unformatted JSON data"
        )
        
        self.parser.add_argument(
            "--config",
            action="store_true",
            help="Display system configuration"
        )
        
        self.parser.add_argument(
            "--set-key",
            nargs=2,
            metavar=("TYPE", "KEY"),
            help="Configure API key (types: openweather, worldtime)"
        )
        
        self.parser.add_argument(
            "--units",
            choices=["metric", "imperial"],
            help="Set measurement units"
        )
        
        self.parser.add_argument(
            "--refresh",
            type=int,
            default=10,
            metavar="SECONDS",
            help="Surveillance refresh interval"
        )
        
        self.parser.add_argument(
            "--clear-cache",
            action="store_true",
            help="Purge cached data"
        )
    
    def execute_configuration(self, args):
        if args.set_key:
            key_type, key_value = args.set_key
            if key_type == "openweather":
                self.resource_mgr.config["openweather_api_key"] = key_value
            elif key_type == "worldtime":
                self.resource_mgr.config["worldtimeapi_key"] = key_value
            self.resource_mgr.save_configuration()
            print(f"Configured {key_type} API key")
            return
        
        if args.units:
            self.resource_mgr.config["units"] = args.units
            os.environ['UNITS'] = args.units
            self.resource_mgr.save_configuration()
            print(f"Units set to {args.units}")
            return
        
        if args.clear_cache:
            if self.resource_mgr.time_db.exists():
                self.resource_mgr.time_db.unlink()
            if self.resource_mgr.weather_db.exists():
                self.resource_mgr.weather_db.unlink()
            self.resource_mgr.init_temporal_db()
            self.resource_mgr.init_atmospheric_db()
            print("Cache purged")
            return
        
        if args.config:
            print("Current Configuration:")
            for key, value in self.resource_mgr.config.items():
                if "key" in key and value:
                    print(f"  {key}: {'*' * 8}{value[-4:]}")
                else:
                    print(f"  {key}: {value}")
            return
    
    def generate_raw_data(self, city_data):
        data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "system": "temporal-atmospheric-matrix",
            "units": self.resource_mgr.config.get("units", "metric"),
            "data": {}
        }
        
        for city_id, (temp, atmos) in city_data.items():
            config = TemporalAcquisition.CITIES[city_id]
            data["data"][city_id] = {
                "display_name": config.display_name,
                "time": {
                    "value": temp.time_str,
                    "source": temp.source.value,
                    "timestamp": temp.timestamp
                },
                "weather": {
                    "temperature": atmos.temperature,
                    "condition": atmos.condition,
                    "humidity": atmos.humidity,
                    "wind_speed": atmos.wind_speed,
                    "source": atmos.source.value,
                    "timestamp": atmos.timestamp
                },
                "coordinates": config.coordinates,
                "timezone": config.timezone
            }
        
        print(json.dumps(data, indent=2))
    
    def execute_surveillance_cycle(self, args):
        if args.compare:
            city_data = {}
            for city_id in TemporalAcquisition.CITIES:
                try:
                    temp = self.temporal_engine.acquire_temporal(city_id)
                    atmos = self.atmospheric_engine.acquire_atmospheric(city_id)
                    city_data[city_id] = (temp, atmos)
                except Exception as e:
                    print(f"Error acquiring data for {city_id}: {e}")
            
            if args.raw:
                self.generate_raw_data(city_data)
            else:
                matrix = self.display_engine.generate_comparative_matrix(city_data)
                print(f"\nTemporal-Atmospheric Comparison Matrix")
                print(f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
                print(matrix)
            return
        
        if args.city == "all":
            if args.raw:
                city_data = {}
                for city_id in TemporalAcquisition.CITIES:
                    temp = self.temporal_engine.acquire_temporal(city_id)
                    atmos = self.atmospheric_engine.acquire_atmospheric(city_id)
                    city_data[city_id] = (temp, atmos)
                self.generate_raw_data(city_data)
            else:
                for city_id in TemporalAcquisition.CITIES:
                    try:
                        temp = self.temporal_engine.acquire_temporal(city_id)
                        atmos = self.atmospheric_engine.acquire_atmospheric(city_id)
                        config = TemporalAcquisition.CITIES[city_id]
                        matrix = self.display_engine.generate_city_matrix(temp, atmos, config)
                        print(matrix + "\n")
                    except Exception as e:
                        print(f"Error displaying {city_id}: {e}")
        else:
            try:
                temp = self.temporal_engine.acquire_temporal(args.city)
                atmos = self.atmospheric_engine.acquire_atmospheric(args.city)
                config = TemporalAcquisition.CITIES[args.city]
                
                if args.raw:
                    self.generate_raw_data({args.city: (temp, atmos)})
                else:
                    matrix = self.display_engine.generate_city_matrix(temp, atmos, config)
                    print(matrix)
            except Exception as e:
                print(f"Error: {e}")
    
    def execute_continuous_surveillance(self, args):
        try:
            interval = max(2, args.refresh)
            cycle = 0
            
            while True:
                os.system('cls' if os.name == 'nt' else 'clear')
                print(f"Temporal-Atmospheric Surveillance - Cycle {cycle}")
                print(f"Refresh: {interval}s | {datetime.utcnow().strftime('%H:%M:%S UTC')}")
                print("─" * 60 + "\n")
                
                self.execute_surveillance_cycle(args)
                
                print(f"\n{'─' * 60}")
                print(f"Next update in {interval}s | Ctrl+C to terminate")
                time.sleep(interval)
                cycle += 1
        except KeyboardInterrupt:
            print("\nSurveillance terminated")
    
    def execute(self):
        args = self.parser.parse_args()
        
        if args.config or args.set_key or args.units or args.clear_cache:
            self.execute_configuration(args)
            return
        
        if args.watch:
            self.execute_continuous_surveillance(args)
        else:
            self.execute_surveillance_cycle(args)

def main():
    try:
        import pytz
    except ImportError:
        print("Error: Required modules not installed.")
        print("Please run: pip install pytz requests urllib3")
        sys.exit(1)
    
    try:
        interface = CommandInterface()
        interface.execute()
    except KeyboardInterrupt:
        print("\nOperation terminated")
        sys.exit(0)
    except Exception as e:
        print(f"System error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
