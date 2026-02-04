/* =========================================================
   FIXED BULK INSERT PATTERN (works for small CSVs too)
   - Adds ROWTERMINATOR + CODEPAGE for ALL tables
   - Keeps your exact stored procedure style
========================================================= */

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE 
        @start_time DATETIME, 
        @end_time DATETIME, 
        @batch_start_time DATETIME, 
        @batch_end_time DATETIME; 

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '================================================';
        PRINT 'Loading Bronze Layer - Flight Crew Project';
        PRINT '================================================';

        PRINT '------------------------------------------------';
        PRINT 'Loading Flight Crew Source Tables';
        PRINT '------------------------------------------------';

        /* =========================
           AIRCRAFT
        ========================== */
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.aircraft';
        TRUNCATE TABLE bronze.aircraft;

        PRINT '>> Inserting Data Into: bronze.aircraft';
        BULK INSERT bronze.aircraft
        FROM 'C:\flight management\dataset\raw_aircraft.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            CODEPAGE = '65001',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        /* =========================
           AIRPORTS
        ========================== */
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.airports';
        TRUNCATE TABLE bronze.airports;

        PRINT '>> Inserting Data Into: bronze.airports';
        BULK INSERT bronze.airports
        FROM 'C:\flight management\dataset\raw_airports.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            CODEPAGE = '65001',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        /* =========================
           CREW
        ========================== */
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crew';
        TRUNCATE TABLE bronze.crew;

        PRINT '>> Inserting Data Into: bronze.crew';
        BULK INSERT bronze.crew
        FROM 'C:\flight management\dataset\raw_crew.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            CODEPAGE = '65001',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        /* =========================
           CREW ASSIGNMENTS
        ========================== */
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crew_assignments';
        TRUNCATE TABLE bronze.crew_assignments;

        PRINT '>> Inserting Data Into: bronze.crew_assignments';
        BULK INSERT bronze.crew_assignments
        FROM 'C:\flight management\dataset\raw_crew_assignments.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            CODEPAGE = '65001',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        /* =========================
           DELAY EVENTS
        ========================== */
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.delay_events';
        TRUNCATE TABLE bronze.delay_events;

        PRINT '>> Inserting Data Into: bronze.delay_events';
        BULK INSERT bronze.delay_events
        FROM 'C:\flight management\dataset\raw_delay_events.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            CODEPAGE = '65001',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        /* =========================
           FLIGHTS
        ========================== */
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.flights';
        TRUNCATE TABLE bronze.flights;

        PRINT '>> Inserting Data Into: bronze.flights';
        BULK INSERT bronze.flights
        FROM 'C:\flight management\dataset\raw_flights.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            CODEPAGE = '65001',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        /* =========================
           WEATHER WINDOWS
        ========================== */
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.weather_windows';
        TRUNCATE TABLE bronze.weather_windows;

        PRINT '>> Inserting Data Into: bronze.weather_windows';
        BULK INSERT bronze.weather_windows
        FROM 'C:\flight management\dataset\raw_weather_windows.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            CODEPAGE = '65001',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        SET @batch_end_time = GETDATE();
        PRINT '==========================================';
        PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '==========================================';
    END TRY
    BEGIN CATCH
        PRINT '==========================================';
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State  : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==========================================';
        THROW;
    END CATCH
END;
GO
