# Databricks notebook source
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import numpy as np
import pytz

def clean_and_merge_data(dfs, crs_target='EPSG:4326', time_cols=[]):
    """
    1. Removes GPS outliers (constrains points to plausible NYC bounds).
    2. Drops records with missing coordinates/timestamps.
    3. Normalizes all geometries to target CRS (default: WGS84).
    4. Converts all specified time columns to UTC, inferring local NYC if tz-naive.
    5. Merges sources on common time/location keys, or concatenates if no shared keys.

    Args:
        dfs: List of pandas or GeoPandas DataFrames.
        crs_target: Target coordinate reference system (default: 'EPSG:4326').
        time_cols: List of time column names to standardize.

    Returns:
        GeoDataFrame: Cleaned and merged.
    """
    cleaned_dfs = []
    for df in dfs:
        # Create geometry from lat/lon if needed
        if 'geometry' not in df.columns:
            lat_cols = [c for c in df.columns if 'lat' in c.lower()]
            lon_cols = [c for c in df.columns if 'lon' in c.lower() or 'lng' in c.lower()]
            if lat_cols and lon_cols:
                lat, lon = lat_cols[0], lon_cols[0]
                df = df.dropna(subset=[lat, lon])
                geometry = [Point(xy) for xy in zip(df[lon], df[lat])]
                gdf = gpd.GeoDataFrame(df, geometry=geometry, crs='EPSG:4326')
            else:
                gdf = gpd.GeoDataFrame(df)
        else:
            gdf = gpd.GeoDataFrame(df)

        # Normalize coordinate system
        if gdf.crs is None:
            gdf.set_crs(crs_target, inplace=True)
        if gdf.crs.to_string() != crs_target:
            try:
                gdf = gdf.to_crs(crs_target)
            except Exception:
                pass  # Keep original if CRS fails

        # GPS noise/outliers: restrict to plausible NYC bounding box
        gdf = gdf[
            (gdf.geometry.y >= 40.4774) & (gdf.geometry.y <= 40.9176) &
            (gdf.geometry.x >= -74.2591) & (gdf.geometry.x <= -73.7004)
        ]

        # Clean and sync timestamps
        for time_col in time_cols:
            if time_col in gdf.columns:
                gdf[time_col] = pd.to_datetime(gdf[time_col], errors='coerce')
                # Localize as NYC if tz-naive, then convert to UTC
                if gdf[time_col].dt.tz is None:
                    nyc = pytz.timezone('America/New_York')
                    gdf[time_col] = gdf[time_col].dt.tz_localize(nyc, ambiguous='NaT', nonexistent='NaT').dt.tz_convert('UTC')
                else:
                    gdf[time_col] = gdf[time_col].dt.tz_convert('UTC')
                gdf = gdf.dropna(subset=[time_col])  # Remove rows missing timestamps

        cleaned_dfs.append(gdf)

    # Merge or concatenate on available keys: all time_cols and geometry if present
    merge_cols = [col for col in time_cols if all(col in df.columns for df in cleaned_dfs)]
    if all('geometry' in df.columns for df in cleaned_dfs):
        merge_cols.append('geometry')

    if merge_cols:
        # Successively merge all DataFrames on merge_cols
        merged = cleaned_dfs[0]
        for other in cleaned_dfs[1:]:
            merged = merged.merge(other, on=merge_cols, how='inner', suffixes=(None, '_dup'))
    else:
        # Just concatenate if no shared columns
        merged = pd.concat(cleaned_dfs, ignore_index=True)

    return gpd.GeoDataFrame(merged, geometry='geometry')


# COMMAND ----------

