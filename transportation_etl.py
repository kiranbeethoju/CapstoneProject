#!/usr/bin/env python3
"""
Transportation Analytics ETL Pipeline
Processes Azure SQL Database data for advanced analytics and visualizations
"""

import os
import sys
from pathlib import Path
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
import json
from typing import Dict, List, Tuple
import warnings
warnings.filterwarnings('ignore')

# Add project root to path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from config.azure_database import (
    test_azure_connection, 
    get_azure_tables, 
    get_table_info, 
    get_table_row_count,
    get_azure_db_connection,
    azure_engine
)
from sqlalchemy import text

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TransportationETL:
    """ETL Pipeline for Transportation Analytics"""
    
    def __init__(self):
        self.connection = None
        self.tables = []
        self.processed_data = {}
        self.connect()
    
    def connect(self):
        """Connect to Azure SQL Database"""
        if test_azure_connection():
            self.connection = get_azure_db_connection()
            self.tables = get_azure_tables()
            logger.info("✅ Connected to Azure SQL Database")
            return True
        else:
            logger.error("❌ Failed to connect to Azure SQL Database")
            return False
    
    def get_table_structure(self, schema: str, table_name: str) -> List[Dict]:
        """Get table structure information"""
        try:
            columns = get_table_info(schema, table_name)
            return [
                {
                    'name': col[0],
                    'type': col[1],
                    'nullable': col[2],
                    'default': col[3]
                }
                for col in columns
            ]
        except Exception as e:
            logger.error(f"❌ Error getting table structure for {schema}.{table_name}: {e}")
            return []
    
    def sample_data(self, schema: str, table_name: str, limit: int = 1000) -> pd.DataFrame:
        """Sample data from a table"""
        try:
            query = f"SELECT TOP {limit} * FROM [{schema}].[{table_name}]"
            df = pd.read_sql(query, self.connection)
            logger.info(f"✅ Sampled {len(df)} rows from {schema}.{table_name}")
            return df
        except Exception as e:
            logger.error(f"❌ Error sampling data from {schema}.{table_name}: {e}")
            return pd.DataFrame()
    
    def process_taxi_data(self) -> Dict:
        """Process taxi data for analytics"""
        logger.info("🚕 Processing taxi data...")
        
        taxi_data = {}
        
        # Process yellow taxi data
        try:
            yellow_query = """
            SELECT
                tpep_pickup_datetime as pickup_datetime,
                tpep_dropoff_datetime as dropoff_datetime,
                passenger_count,
                trip_distance,
                RatecodeID,
                PULocationID,
                DOLocationID,
                payment_type,
                fare_amount,
                extra,
                mta_tax,
                tip_amount,
                tolls_amount,
                improvement_surcharge,
                total_amount,
                congestion_surcharge
            FROM dbo.yellow_tripdata
            WHERE tpep_pickup_datetime IS NOT NULL 
            AND tpep_dropoff_datetime IS NOT NULL
            """
            
            yellow_df = pd.read_sql(yellow_query, self.connection)
            
            if not yellow_df.empty:
                # Clean and process data
                yellow_df = self._clean_taxi_data(yellow_df)
                yellow_df['taxi_type'] = 'yellow'
                
                taxi_data['yellow'] = yellow_df
                logger.info(f"✅ Processed {len(yellow_df)} yellow taxi records")
            
        except Exception as e:
            logger.error(f"❌ Error processing yellow taxi data: {e}")
        
        # Process green taxi data
        try:
            green_query = """
            SELECT
                lpep_pickup_datetime as pickup_datetime,
                lpep_dropoff_datetime as dropoff_datetime,
                passenger_count,
                trip_distance,
                RatecodeID,
                PULocationID,
                DOLocationID,
                payment_type,
                fare_amount,
                extra,
                mta_tax,
                tip_amount,
                tolls_amount,
                improvement_surcharge,
                total_amount,
                congestion_surcharge
            FROM dbo.green_tripdata_2025
            WHERE lpep_pickup_datetime IS NOT NULL 
            AND lpep_dropoff_datetime IS NOT NULL
            """
            
            green_df = pd.read_sql(green_query, self.connection)
            
            if not green_df.empty:
                # Clean and process data
                green_df = self._clean_taxi_data(green_df)
                green_df['taxi_type'] = 'green'
                
                taxi_data['green'] = green_df
                logger.info(f"✅ Processed {len(green_df)} green taxi records")
            
        except Exception as e:
            logger.error(f"❌ Error processing green taxi data: {e}")
        
        # Process FHV data
        try:
            fhv_query = """
            SELECT
                pickup_datetime,
                dropOff_datetime as dropoff_datetime,
                PUlocationID,
                DOlocationID,
                SR_Flag,
                Affiliated_base_number
            FROM [dbo].[fhv-tripdata]
            WHERE pickup_datetime IS NOT NULL 
            AND dropOff_datetime IS NOT NULL
            """
            
            fhv_df = pd.read_sql(fhv_query, self.connection)
            
            if not fhv_df.empty:
                # Clean and process data
                fhv_df = self._clean_taxi_data(fhv_df)
                fhv_df['taxi_type'] = 'fhv'
                
                taxi_data['fhv'] = fhv_df
                logger.info(f"✅ Processed {len(fhv_df)} FHV records")
            
        except Exception as e:
            logger.error(f"❌ Error processing FHV data: {e}")
        
        return taxi_data
    
    def _clean_taxi_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean taxi data"""
        if df.empty:
            return df
        
        # Convert numeric columns
        numeric_cols = ['passenger_count', 'trip_distance', 'fare_amount', 'tip_amount', 'total_amount']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Remove invalid passenger counts
        if 'passenger_count' in df.columns:
            df = df[df['passenger_count'] > 0]
            df = df[df['passenger_count'] <= 10]
        
        # Remove invalid trip distances
        if 'trip_distance' in df.columns:
            df = df[df['trip_distance'] > 0]
            df = df[df['trip_distance'] < 100]  # Max 100 miles
        
        # Remove invalid fares
        if 'fare_amount' in df.columns:
            df = df[df['fare_amount'] > 0]
            df = df[df['fare_amount'] < 1000]  # Max $1000
        
        # Convert datetime columns
        datetime_cols = ['pickup_datetime', 'dropoff_datetime']
        for col in datetime_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Remove rows with invalid dates
        df = df.dropna(subset=['pickup_datetime', 'dropoff_datetime'])
        
        # Calculate trip duration
        df['trip_duration'] = (df['dropoff_datetime'] - df['pickup_datetime']).dt.total_seconds() / 60
        
        # Remove invalid trip durations
        df = df[df['trip_duration'] > 0]
        df = df[df['trip_duration'] < 1440]  # Max 24 hours
        
        # Add time-based features
        df['pickup_hour'] = df['pickup_datetime'].dt.hour
        df['pickup_day'] = df['pickup_datetime'].dt.day_name()
        df['pickup_month'] = df['pickup_datetime'].dt.month
        df['pickup_weekday'] = df['pickup_datetime'].dt.weekday
        
        # Calculate speed (if distance available)
        if 'trip_distance' in df.columns and 'trip_duration' in df.columns:
            df['avg_speed_mph'] = (df['trip_distance'] / (df['trip_duration'] / 60)).fillna(0)
            df = df[df['avg_speed_mph'] < 100]  # Remove unrealistic speeds
        
        # Add location-based features using zone IDs
        if 'PULocationID' in df.columns:
            df['pickup_zone'] = df['PULocationID']
        if 'DOLocationID' in df.columns:
            df['dropoff_zone'] = df['DOLocationID']
        
        return df
    
    def process_subway_data(self) -> pd.DataFrame:
        """Process subway data for analytics"""
        logger.info("🚇 Processing subway data...")
        
        try:
            # Try different table names
            subway_tables = [
                'dbo.MTA_Subway_Turnstile_Usage_Data_2022_20250720',
                'dbo.mta_1706'
            ]
            
            subway_df = pd.DataFrame()
            
            for table_name in subway_tables:
                try:
                    # Get table structure first
                    structure = self.get_table_structure('dbo', table_name.split('.')[-1])
                    
                    if structure:
                        # Build query based on available columns
                        columns = [col['name'] for col in structure]
                        
                        if 'linename' in columns:
                            query = f"""
                            SELECT TOP 5000
                                linename,
                                station,
                                COUNT(*) as entry_count
                            FROM {table_name}
                            WHERE linename IS NOT NULL
                            GROUP BY linename, station
                            ORDER BY entry_count DESC
                            """
                        else:
                            query = f"SELECT TOP 1000 * FROM {table_name}"
                        
                        df = pd.read_sql(query, self.connection)
                        
                        if not df.empty:
                            subway_df = df
                            logger.info(f"✅ Processed subway data from {table_name}")
                            break
                
                except Exception as e:
                    logger.warning(f"⚠️ Could not process {table_name}: {e}")
                    continue
            
            return subway_df
            
        except Exception as e:
            logger.error(f"❌ Error processing subway data: {e}")
            return pd.DataFrame()
    
    def process_bikeshare_data(self) -> pd.DataFrame:
        """Process bikeshare data for analytics"""
        logger.info("🚲 Processing bikeshare data...")
        
        try:
            bikeshare_query = """
            SELECT
                STATION_NAME,
                ADDRESS,
                CITY,
                STATE,
                LATITUDE,
                LONGITUDE,
                STATION_TYPE,
                SYSTEM_NAME,
                YEAR,
                ASOFDATE
            FROM [dbo].[NTAD_Bikeshare_-657912010002967768]
            WHERE LATITUDE IS NOT NULL 
            AND LONGITUDE IS NOT NULL
            """
            
            bikeshare_df = pd.read_sql(bikeshare_query, self.connection)
            
            if not bikeshare_df.empty:
                # Clean and process data
                bikeshare_df = self._clean_bikeshare_data(bikeshare_df)
                logger.info(f"✅ Processed {len(bikeshare_df)} bikeshare records")
            
            return bikeshare_df
            
        except Exception as e:
            logger.error(f"❌ Error processing bikeshare data: {e}")
            return pd.DataFrame()
    
    def _clean_bikeshare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean bikeshare data"""
        if df.empty:
            return df
        
        # Convert coordinates to numeric
        if 'LATITUDE' in df.columns:
            df['LATITUDE'] = pd.to_numeric(df['LATITUDE'], errors='coerce')
        if 'LONGITUDE' in df.columns:
            df['LONGITUDE'] = pd.to_numeric(df['LONGITUDE'], errors='coerce')
        
        # Remove invalid coordinates (NYC area)
        df = df[
            (df['LATITUDE'].between(40.5, 41.0)) &
            (df['LONGITUDE'].between(-74.3, -73.7))
        ]
        
        # Convert date column
        if 'ASOFDATE' in df.columns:
            df['ASOFDATE'] = pd.to_datetime(df['ASOFDATE'], errors='coerce')
        
        # Add geographic features
        df['station_location'] = df.apply(
            lambda row: f"{row['STATION_NAME']}, {row['CITY']}" if pd.notna(row['STATION_NAME']) else "Unknown",
            axis=1
        )
        
        # Add year-based features
        if 'YEAR' in df.columns:
            df['year_category'] = df['YEAR'].apply(
                lambda x: 'Recent' if x >= 2020 else 'Historical' if x >= 2015 else 'Legacy'
            )
        
        return df
    
    def process_taxi_zones(self) -> pd.DataFrame:
        """Process taxi zones data"""
        logger.info("🗺️ Processing taxi zones data...")
        
        try:
            zones_query = """
            SELECT *
            FROM dbo.taxi_zone_lookup
            """
            
            zones_df = pd.read_sql(zones_query, self.connection)
            logger.info(f"✅ Processed {len(zones_df)} taxi zone records")
            
            return zones_df
            
        except Exception as e:
            logger.error(f"❌ Error processing taxi zones data: {e}")
            return pd.DataFrame()
    
    def generate_analytics(self) -> Dict:
        """Generate comprehensive analytics"""
        logger.info("📊 Generating analytics...")
        
        analytics = {}
        
        # Process all data types
        taxi_data = self.process_taxi_data()
        subway_data = self.process_subway_data()
        bikeshare_data = self.process_bikeshare_data()
        zones_data = self.process_taxi_zones()
        
        # Store processed data
        self.processed_data = {
            'taxi': taxi_data,
            'subway': subway_data,
            'bikeshare': bikeshare_data,
            'zones': zones_data
        }
        
        # Generate analytics for each mode
        if taxi_data:
            analytics['taxi'] = self._analyze_taxi_data(taxi_data)
        
        if not subway_data.empty:
            analytics['subway'] = self._analyze_subway_data(subway_data)
        
        if not bikeshare_data.empty:
            analytics['bikeshare'] = self._analyze_bikeshare_data(bikeshare_data)
        
        # Generate cross-modal analytics
        analytics['cross_modal'] = self._analyze_cross_modal_data()
        
        return analytics
    
    def _analyze_taxi_data(self, taxi_data: Dict) -> Dict:
        """Analyze taxi data"""
        analytics = {}
        
        # Combine all taxi types
        all_taxi = pd.concat([df for df in taxi_data.values()], ignore_index=True)
        
        if not all_taxi.empty:
            # Temporal analysis
            analytics['hourly_distribution'] = all_taxi['pickup_hour'].value_counts().sort_index().to_dict()
            analytics['daily_distribution'] = all_taxi['pickup_day'].value_counts().to_dict()
            analytics['monthly_distribution'] = all_taxi['pickup_month'].value_counts().sort_index().to_dict()
            
            # Geographic analysis
            analytics['pickup_hotspots'] = self._identify_hotspots(all_taxi, 'pickup')
            analytics['dropoff_hotspots'] = self._identify_hotspots(all_taxi, 'dropoff')
            
            # Performance metrics
            if 'trip_duration' in all_taxi.columns:
                analytics['avg_trip_duration'] = all_taxi['trip_duration'].mean()
                analytics['median_trip_duration'] = all_taxi['trip_duration'].median()
            
            if 'fare_amount' in all_taxi.columns:
                analytics['avg_fare'] = all_taxi['fare_amount'].mean()
                analytics['median_fare'] = all_taxi['fare_amount'].median()
            
            if 'avg_speed_mph' in all_taxi.columns:
                analytics['avg_speed'] = all_taxi['avg_speed_mph'].mean()
            
            # Type analysis
            analytics['type_distribution'] = all_taxi['taxi_type'].value_counts().to_dict()
        
        return analytics
    
    def _analyze_subway_data(self, subway_data: pd.DataFrame) -> Dict:
        """Analyze subway data"""
        analytics = {}
        
        if not subway_data.empty:
            # Line analysis
            if 'linename' in subway_data.columns:
                analytics['line_distribution'] = subway_data['linename'].value_counts().head(10).to_dict()
            
            # Station analysis
            if 'station' in subway_data.columns:
                analytics['station_distribution'] = subway_data['station'].value_counts().head(10).to_dict()
            
            # Entry count analysis
            if 'entry_count' in subway_data.columns:
                analytics['total_entries'] = subway_data['entry_count'].sum()
                analytics['avg_entries_per_station'] = subway_data['entry_count'].mean()
        
        return analytics
    
    def _analyze_bikeshare_data(self, bikeshare_data: pd.DataFrame) -> Dict:
        """Analyze bikeshare data"""
        analytics = {}
        
        if not bikeshare_data.empty:
            # Temporal analysis
            if 'start_hour' in bikeshare_data.columns:
                analytics['hourly_distribution'] = bikeshare_data['start_hour'].value_counts().sort_index().to_dict()
            
            if 'start_day' in bikeshare_data.columns:
                analytics['daily_distribution'] = bikeshare_data['start_day'].value_counts().to_dict()
            
            # Station analysis
            if 'start_station_name' in bikeshare_data.columns:
                analytics['popular_start_stations'] = bikeshare_data['start_station_name'].value_counts().head(10).to_dict()
            
            if 'end_station_name' in bikeshare_data.columns:
                analytics['popular_end_stations'] = bikeshare_data['end_station_name'].value_counts().head(10).to_dict()
            
            # Trip analysis
            if 'trip_duration' in bikeshare_data.columns:
                analytics['avg_trip_duration'] = bikeshare_data['trip_duration'].mean()
                analytics['median_trip_duration'] = bikeshare_data['trip_duration'].median()
            
            # User type analysis
            if 'user_type' in bikeshare_data.columns:
                analytics['user_type_distribution'] = bikeshare_data['user_type'].value_counts().to_dict()
        
        return analytics
    
    def _identify_hotspots(self, df: pd.DataFrame, location_type: str) -> List[Dict]:
        """Identify geographic hotspots"""
        hotspots = []
        
        try:
            lat_col = f'{location_type}_latitude'
            lon_col = f'{location_type}_longitude'
            
            if lat_col in df.columns and lon_col in df.columns:
                # Group by rounded coordinates to identify clusters
                df['lat_rounded'] = df[lat_col].round(3)
                df['lon_rounded'] = df[lon_col].round(3)
                
                hotspot_groups = df.groupby(['lat_rounded', 'lon_rounded']).size().reset_index(name='count')
                hotspot_groups = hotspot_groups.sort_values('count', ascending=False).head(20)
                
                for _, row in hotspot_groups.iterrows():
                    hotspots.append({
                        'latitude': row['lat_rounded'],
                        'longitude': row['lon_rounded'],
                        'count': int(row['count'])
                    })
        
        except Exception as e:
            logger.error(f"❌ Error identifying {location_type} hotspots: {e}")
        
        return hotspots
    
    def _analyze_cross_modal_data(self) -> Dict:
        """Analyze cross-modal patterns"""
        analytics = {}
        
        # Compare temporal patterns across modes
        temporal_comparison = {}
        
        # Taxi temporal patterns
        if 'taxi' in self.processed_data and self.processed_data['taxi']:
            all_taxi = pd.concat([df for df in self.processed_data['taxi'].values()], ignore_index=True)
            if not all_taxi.empty and 'pickup_hour' in all_taxi.columns:
                temporal_comparison['taxi'] = all_taxi['pickup_hour'].value_counts().sort_index().to_dict()
        
        # Bikeshare temporal patterns
        if 'bikeshare' in self.processed_data and not self.processed_data['bikeshare'].empty:
            bikeshare = self.processed_data['bikeshare']
            if 'start_hour' in bikeshare.columns:
                temporal_comparison['bikeshare'] = bikeshare['start_hour'].value_counts().sort_index().to_dict()
        
        analytics['temporal_comparison'] = temporal_comparison
        
        return analytics
    
    def save_processed_data(self, output_dir: str = "data/processed"):
        """Save processed data to files"""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"💾 Saving processed data to {output_path}")
        
        # Save taxi data
        if 'taxi' in self.processed_data:
            for taxi_type, df in self.processed_data['taxi'].items():
                if not df.empty:
                    file_path = output_path / f"taxi_{taxi_type}.parquet"
                    df.to_parquet(file_path, index=False)
                    logger.info(f"✅ Saved {taxi_type} taxi data: {len(df)} records")
        
        # Save subway data
        if 'subway' in self.processed_data and not self.processed_data['subway'].empty:
            file_path = output_path / "subway.parquet"
            self.processed_data['subway'].to_parquet(file_path, index=False)
            logger.info(f"✅ Saved subway data: {len(self.processed_data['subway'])} records")
        
        # Save bikeshare data
        if 'bikeshare' in self.processed_data and not self.processed_data['bikeshare'].empty:
            file_path = output_path / "bikeshare.parquet"
            self.processed_data['bikeshare'].to_parquet(file_path, index=False)
            logger.info(f"✅ Saved bikeshare data: {len(self.processed_data['bikeshare'])} records")
        
        # Save zones data
        if 'zones' in self.processed_data and not self.processed_data['zones'].empty:
            file_path = output_path / "taxi_zones.parquet"
            self.processed_data['zones'].to_parquet(file_path, index=False)
            logger.info(f"✅ Saved taxi zones data: {len(self.processed_data['zones'])} records")
    
    def save_analytics(self, analytics: Dict, output_dir: str = "data/analytics"):
        """Save analytics results to JSON"""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        file_path = output_path / "transportation_analytics.json"
        
        # Convert numpy types to native Python types for JSON serialization
        def convert_numpy_types(obj):
            if isinstance(obj, np.integer):
                return int(obj)
            elif isinstance(obj, np.floating):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            elif isinstance(obj, dict):
                return {key: convert_numpy_types(value) for key, value in obj.items()}
            elif isinstance(obj, list):
                return [convert_numpy_types(item) for item in obj]
            else:
                return obj
        
        analytics_serializable = convert_numpy_types(analytics)
        
        with open(file_path, 'w') as f:
            json.dump(analytics_serializable, f, indent=2, default=str)
        
        logger.info(f"✅ Saved analytics to {file_path}")

def main():
    """Main ETL pipeline execution"""
    print("🚀 Starting Transportation Analytics ETL Pipeline")
    print("=" * 60)
    
    # Initialize ETL pipeline
    etl = TransportationETL()
    
    if not etl.connect():
        print("❌ Cannot proceed without database connection")
        return
    
    # Generate analytics
    print("📊 Generating comprehensive analytics...")
    analytics = etl.generate_analytics()
    
    # Save processed data
    print("💾 Saving processed data...")
    etl.save_processed_data()
    
    # Save analytics
    print("📈 Saving analytics results...")
    etl.save_analytics(analytics)
    
    # Print summary
    print("\n📋 ETL PIPELINE SUMMARY")
    print("=" * 60)
    
    if 'taxi' in analytics:
        taxi_data = etl.processed_data.get('taxi', {})
        total_taxi = sum(len(df) for df in taxi_data.values())
        print(f"🚕 Taxi Data: {total_taxi:,} records processed")
        print(f"   - Yellow: {len(taxi_data.get('yellow', pd.DataFrame())):,}")
        print(f"   - Green: {len(taxi_data.get('green', pd.DataFrame())):,}")
        print(f"   - FHV: {len(taxi_data.get('fhv', pd.DataFrame())):,}")
    
    if 'subway' in analytics:
        subway_data = etl.processed_data.get('subway', pd.DataFrame())
        print(f"🚇 Subway Data: {len(subway_data):,} records processed")
    
    if 'bikeshare' in analytics:
        bikeshare_data = etl.processed_data.get('bikeshare', pd.DataFrame())
        print(f"🚲 Bikeshare Data: {len(bikeshare_data):,} records processed")
    
    if 'zones' in etl.processed_data:
        zones_data = etl.processed_data.get('zones', pd.DataFrame())
        print(f"🗺️ Taxi Zones: {len(zones_data):,} records processed")
    
    print("\n✅ ETL Pipeline completed successfully!")
    print("📁 Check 'data/processed/' for processed data files")
    print("📊 Check 'data/analytics/' for analytics results")

if __name__ == "__main__":
    main() 