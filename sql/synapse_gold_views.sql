-- ============================================
-- YTMusic Gold Layer — Synapse Serverless SQL Setup
-- ============================================

-- Step 1: Create the database (skip if already created)
CREATE DATABASE ytmusic_gold;
GO

-- Step 2: Switch context to the new database
USE ytmusic_gold;
GO

-- Step 3: Create views over Gold Delta tables

CREATE VIEW dbo.artist_summary AS
SELECT *
FROM OPENROWSET(
    BULK 'https://adlsytmusic.dfs.core.windows.net/gold/artist_summary/',
    FORMAT = 'DELTA'
) AS result;
GO

CREATE VIEW dbo.country_top10 AS
SELECT *
FROM OPENROWSET(
    BULK 'https://adlsytmusic.dfs.core.windows.net/gold/country_top10/',
    FORMAT = 'DELTA'
) AS result;
GO

CREATE VIEW dbo.recent_song_counts AS
SELECT *
FROM OPENROWSET(
    BULK 'https://adlsytmusic.dfs.core.windows.net/gold/recent_song_counts/',
    FORMAT = 'DELTA'
) AS result;
GO

-- Step 4: Sanity check all three views
SELECT * FROM dbo.artist_summary;
SELECT * FROM dbo.country_top10;
SELECT * FROM dbo.recent_song_counts;
