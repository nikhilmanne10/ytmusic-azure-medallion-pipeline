%pip install azure-storage-file-datalake deltalake pandas
%pip install azure-storage-file-datalake deltalake pandas pyarrow
dbutils.library.restartPython()
# CELL 1
%pip install deltalake pandas
%pip install deltalake pandas pyarrow
# CELL 2
dbutils.library.restartPython()
# CELL 3 — Read Bronze JSON (dynamic date, matches today's ingestion automatically)
from azure.storage.filedatalake import DataLakeServiceClient
import json
import pandas as pd
from datetime import datetime

storage_account = "adlsytmusic"
storage_key = "PASTE_YOUR_KEY_HERE"  # set via Key Vault / env var in production

service_client = DataLakeServiceClient(
    account_url=f"https://{storage_account}.dfs.core.windows.net",
    credential=storage_key
)

def read_json_from_adls(container, file_path):
    file_system_client = service_client.get_file_system_client(file_system=container)
    file_client = file_system_client.get_file_client(file_path)
    download = file_client.download_file()
    content = download.readall()
    return json.loads(content)

# ---- Dynamic date: always matches today's Bronze ingestion folder ----
date_path = datetime.utcnow().strftime("%Y/%m/%d")
print(f"Reading Bronze data for: {date_path}")

charts_raw = read_json_from_adls("bronze", f"ytmusic/charts/{date_path}/charts_raw.json")
artists_raw = read_json_from_adls("bronze", f"ytmusic/artists/{date_path}/artists_raw.json")
songs_raw = read_json_from_adls("bronze", f"ytmusic/songs/{date_path}/songs_raw.json")

print("Loaded charts, artists, and songs raw data.")
# CELL 4 — Flatten charts
rows = []
for country, chart in charts_raw["charts_by_country"].items():
    artist_list = chart.get("artists", {})
    items = artist_list.get("items", []) if isinstance(artist_list, dict) else artist_list
    for idx, artist in enumerate(items, start=1):
        rows.append({
            "country": country,
            "rank": idx,
            "artist_name": artist.get("title") or artist.get("artist") or str(artist),
            "ingestion_date": date_path.replace("/", "-")
        })

charts_pdf = pd.DataFrame(rows)
print(f"Charts rows: {len(charts_pdf)}")
# CELL 5 — Flatten songs with release year
song_rows = []
for song in songs_raw["songs"]:
    song_rows.append({
        "artist_name": song.get("artist_name"),
        "song_title": song.get("song_title"),
        "album_name": song.get("album_name"),
        "video_id": song.get("video_id"),
        "year": song.get("year"),
        "ingestion_date": date_path.replace("/", "-")
    })

songs_pdf = pd.DataFrame(song_rows)
songs_pdf["year"] = pd.to_numeric(songs_pdf["year"], errors="coerce")
print(f"Songs rows: {len(songs_pdf)} | with valid year: {songs_pdf['year'].notna().sum()}")
# CELL 6 — PySpark cleaning
charts_sdf = spark.createDataFrame(charts_pdf)
songs_sdf = spark.createDataFrame(songs_pdf)

from pyspark.sql import functions as F

charts_silver = (
    charts_sdf
    .dropDuplicates(["country", "artist_name", "ingestion_date"])
    .withColumn("artist_name", F.trim(F.col("artist_name")))
    .filter(F.col("artist_name").isNotNull())
)

CURRENT_YEAR = datetime.utcnow().year
songs_silver = (
    songs_sdf
    .dropDuplicates(["artist_name", "song_title", "video_id"])
    .withColumn("song_title", F.trim(F.col("song_title")))
    .filter(F.col("year").isNotNull())
    .filter(F.col("year") >= CURRENT_YEAR - 5)
)

charts_silver.show(10, truncate=False)
songs_silver.show(10, truncate=False)
# CELL 7 — Write Silver tables (pyarrow fix applied)
from deltalake import write_deltalake
import pyarrow as pa

storage_options = {"account_name": storage_account, "account_key": storage_key}

write_deltalake(
    f"abfss://silver@{storage_account}.dfs.core.windows.net/chart_artists",
    pa.Table.from_pandas(charts_silver.toPandas()),
    mode="overwrite",
    storage_options=storage_options
)

write_deltalake(
    f"abfss://silver@{storage_account}.dfs.core.windows.net/recent_songs",
    pa.Table.from_pandas(songs_silver.toPandas()),
    mode="overwrite",
    storage_options=storage_options
)

print("✅ Silver tables written: chart_artists, recent_songs")
