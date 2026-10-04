# CELL 1
%pip install deltalake pandas
%pip install deltalake pandas pyarrow
# CELL 2
dbutils.library.restartPython()
# CELL 3 — Read Silver tables
from deltalake import DeltaTable
import pandas as pd

storage_account = "adlsytmusic"
storage_key = "PASTE_YOUR_KEY_HERE"  # set via Key Vault / env var in production
storage_options = {"account_name": storage_account, "account_key": storage_key}

charts_dt = DeltaTable(f"abfss://silver@{storage_account}.dfs.core.windows.net/chart_artists", storage_options=storage_options)
songs_dt = DeltaTable(f"abfss://silver@{storage_account}.dfs.core.windows.net/recent_songs", storage_options=storage_options)

charts_pdf = charts_dt.to_pandas()
songs_pdf = songs_dt.to_pandas()

print(charts_pdf.shape, songs_pdf.shape)
# CELL 4 — Gold aggregations
charts_sdf = spark.createDataFrame(charts_pdf)
songs_sdf = spark.createDataFrame(songs_pdf)

from pyspark.sql import functions as F

gold_artist_summary = (
    charts_sdf
    .groupBy("artist_name")
    .agg(
        F.countDistinct("country").alias("countries_charted_in"),
        F.min("rank").alias("best_rank"),
        F.avg("rank").alias("avg_rank")
    )
    .orderBy(F.desc("countries_charted_in"), F.asc("best_rank"))
)

gold_country_top10 = (
    charts_sdf
    .filter(F.col("rank") <= 10)
    .orderBy("country", "rank")
)

gold_recent_song_counts = (
    songs_sdf
    .groupBy("artist_name")
    .agg(
        F.count("*").alias("recent_song_count"),
        F.min("year").alias("earliest_recent_year"),
        F.max("year").alias("latest_recent_year")
    )
    .orderBy(F.desc("recent_song_count"))
)

gold_artist_summary.show(20, truncate=False)
gold_country_top10.show(30, truncate=False)
gold_recent_song_counts.show(20, truncate=False)
# CELL 5 — Write Gold tables (pyarrow fix applied)
from deltalake import write_deltalake
import pyarrow as pa

write_deltalake(f"abfss://gold@{storage_account}.dfs.core.windows.net/artist_summary",
    pa.Table.from_pandas(gold_artist_summary.toPandas()), mode="overwrite", storage_options=storage_options)

write_deltalake(f"abfss://gold@{storage_account}.dfs.core.windows.net/country_top10",
    pa.Table.from_pandas(gold_country_top10.toPandas()), mode="overwrite", storage_options=storage_options)

write_deltalake(f"abfss://gold@{storage_account}.dfs.core.windows.net/recent_song_counts",
    pa.Table.from_pandas(gold_recent_song_counts.toPandas()), mode="overwrite", storage_options=storage_options)

print("✅ Gold tables written: artist_summary, country_top10, recent_song_counts")
