/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver) - Flight Crew Management
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema for the Flight
    Crew Management project.

    Actions Performed:
        - Truncates Silver tables.
        - Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
    None.

Usage Example:
    EXEC silver.load_silver_flight;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver_flight AS
BEGIN
    DECLARE 
        @start_time DATETIME,
        @end_time DATETIME,
        @batch_start_time DATETIME,
        @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '================================================';
        PRINT 'Loading Silver Layer - Flight Crew Management';
        PRINT '================================================';

        PRINT '------------------------------------------------';
        PRINT 'Loading Dimension Tables';
        PRINT '------------------------------------------------';

        -- =========================================================
        -- Loading silver.airports
        -- =========================================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.airports';
        TRUNCATE TABLE silver.airports;

        PRINT '>> Inserting Data Into: silver.airports';
        ;WITH cleaned AS (
            SELECT
                UPPER(LTRIM(RTRIM(airport_code))) AS airport_code,
                UPPER(LTRIM(RTRIM(state)))        AS state,
                LTRIM(RTRIM(region))              AS region,
                LTRIM(RTRIM(airport_name))        AS airport_name,
                CASE
                    WHEN UPPER(LTRIM(RTRIM(hub_flag))) IN ('TRUE','1','T','YES','Y') THEN 'Yes'
                    WHEN UPPER(LTRIM(RTRIM(hub_flag))) IN ('FALSE','0','F','NO','N') THEN 'No'
                    ELSE 'n/a'
                END AS hub_flag
            FROM bronze.airports
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY airport_code
                    ORDER BY
                        CASE hub_flag
                            WHEN 'Yes' THEN 1
                            WHEN 'No'  THEN 2
                            ELSE 3
                        END,
                        airport_name DESC
                ) AS rn
            FROM cleaned
        )
        INSERT INTO silver.airports (airport_code, state, region, airport_name, hub_flag)
        SELECT
            airport_code,
            state,
            region,
            airport_name,
            hub_flag
        FROM ranked
        WHERE rn = 1;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- =========================================================
        -- Loading silver.aircraft
        -- =========================================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.aircraft';
        TRUNCATE TABLE silver.aircraft;

        PRINT '>> Inserting Data Into: silver.aircraft';
        INSERT INTO silver.aircraft (
            aircraft_id,
            aircraft_type,
            tail_number,
            in_service_date,
            retired_flag
        )
        SELECT
            aircraft_id,
            CASE
                WHEN UPPER(aircraft_type) LIKE '%319%' THEN 'A319'
                WHEN UPPER(aircraft_type) LIKE '%320%' THEN 'A320'
                WHEN UPPER(aircraft_type) LIKE '%321%' THEN 'A321'
                WHEN UPPER(aircraft_type) LIKE '%738%' THEN 'B738'
                WHEN UPPER(aircraft_type) LIKE '%737%' THEN 'B737'
                WHEN UPPER(aircraft_type) LIKE '%757%' THEN 'B757'
                WHEN UPPER(aircraft_type) LIKE '%787%' THEN 'B787'
                WHEN UPPER(aircraft_type) LIKE '%175%' THEN 'E175'
                ELSE 'N/A'
            END AS aircraft_type,
            REPLACE(tail_number, '-', '') AS tail_number,
            COALESCE(
                TRY_CONVERT(date, in_service_date, 101), -- MM/DD/YYYY
                TRY_CONVERT(date, in_service_date, 105)  -- DD-MM-YYYY
            ) AS in_service_date,
            CASE
                WHEN UPPER(LTRIM(RTRIM(retired_flag))) IN ('TRUE','1','Y') THEN 'Yes'
                WHEN UPPER(LTRIM(RTRIM(retired_flag))) IN ('FALSE','0','N') THEN 'No'
                ELSE 'n/a'
            END AS retired_flag
        FROM bronze.aircraft;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- =========================================================
        -- Loading silver.crew
        -- =========================================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crew';
        TRUNCATE TABLE silver.crew;

        PRINT '>> Inserting Data Into: silver.crew';
        INSERT INTO silver.crew (
            crew_id,
            crew_name,
            crew_role,
            base_airport,
            hire_date,
            active_flag
        )
        SELECT 
            crew_id,
            crew_name,
            CASE 
                WHEN UPPER(REPLACE(REPLACE(TRIM(crew_role),'/',''),' ','')) 
                    IN ('FA','FA1','FLIGHTATTENDANT') THEN 'Flight_Attendant'
                WHEN UPPER(REPLACE(REPLACE(TRIM(crew_role),'/',''),' ','')) 
                    IN ('PURSER','PSR') THEN 'Purser'
                WHEN UPPER(REPLACE(REPLACE(TRIM(crew_role),'/',''),' ','')) 
                    IN ('CAPTAIN','CPT','CAPT') THEN 'Captain'
                WHEN UPPER(REPLACE(REPLACE(TRIM(crew_role),'/',''),' ','')) 
                    IN ('FO','F/O','FIRSTOFFICER','FIRSTOFFICER') THEN 'First_Officer'
                ELSE 'Unknown'
            END AS crew_role,
            COALESCE(NULLIF(TRIM(base_airport), ''), 'Unknown') AS base_airport,
            TRY_CAST(hire_date AS date) AS hire_date,
            CASE
                WHEN UPPER(TRIM(active_flag)) IN ('TRUE','1','Y') THEN 'Yes'
                WHEN UPPER(TRIM(active_flag)) IN ('FALSE','0','N') THEN 'No'
                ELSE 'n/a'
            END AS active_flag
        FROM bronze.crew;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';



        PRINT '------------------------------------------------';
        PRINT 'Loading Fact Tables';
        PRINT '------------------------------------------------';

        -- =========================================================
        -- Loading silver.crew_assignments (latest per assignment_id)
        -- =========================================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crew_assignments';
        TRUNCATE TABLE silver.crew_assignments;

        PRINT '>> Inserting Data Into: silver.crew_assignments';
        ;WITH ranked AS (
            SELECT 
                assignment_id,
                crew_id,
                raw_flight_id,
                TRY_CAST(duty_start_ts AS datetime2(0)) AS duty_start_ts,
                TRY_CAST(duty_end_ts AS datetime2(0)) AS duty_end_ts,
                CASE
                    WHEN UPPER(TRIM(reserve_flag)) IN ('TRUE','1','T') THEN 'Yes'
                    WHEN UPPER(TRIM(reserve_flag)) IN ('FALSE','0','F') THEN 'No'
                    ELSE 'n/a'
                END AS reserve_flag,
                assignment_status,
                TRY_CAST(last_updated_ts AS datetime2(0)) AS last_updated_ts,
                ROW_NUMBER() OVER (
                    PARTITION BY assignment_id 
                    ORDER BY TRY_CAST(last_updated_ts AS datetime2(0)) DESC
                ) AS rn
            FROM bronze.crew_assignments
        )
        INSERT INTO silver.crew_assignments (
            assignment_id,
            crew_id,
            raw_flight_id,
            duty_start_ts,
            duty_end_ts,
            reserve_flag,
            assignment_status,
            last_updated_ts
        )
        SELECT 
            assignment_id,
            crew_id,
            raw_flight_id,
            duty_start_ts,
            duty_end_ts,
            reserve_flag,
            assignment_status,
            last_updated_ts
        FROM ranked
        WHERE rn = 1;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- =========================================================
        -- Loading silver.flights (latest per raw_flight_id)
        -- =========================================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.flights';
        TRUNCATE TABLE silver.flights;

        PRINT '>> Inserting Data Into: silver.flights';
        ;WITH ranked AS (
            SELECT 
                raw_flight_id,
                UPPER(REPLACE(LTRIM(RTRIM(flight_number)), ' ', '')) AS flight_number,
                TRY_CAST(flight_date AS date) AS flight_date,
                UPPER(LEFT(REPLACE(TRIM(origin), ' ', ''), 3))      AS origin,
                UPPER(LEFT(REPLACE(TRIM(destination), ' ', ''), 3)) AS destination,
                TRY_CAST(scheduled_dep_ts AS datetime2(0)) AS scheduled_dep_ts,
                TRY_CAST(actual_dep_ts AS datetime2(0)) AS actual_dep_ts,
                TRY_CAST(scheduled_arr_ts AS datetime2(0)) AS scheduled_arr_ts,
                TRY_CAST(actual_arr_ts AS datetime2(0)) AS actual_arr_ts,
                CASE
                    WHEN UPPER(TRIM(status_text)) LIKE '%CANCEL%' THEN 'Yes'
                    WHEN UPPER(TRIM(cancelled_flag)) IN ('TRUE','1','Y') THEN 'Yes'
                    WHEN TRY_CAST(flight_date AS date) < CAST(GETDATE() AS date)
                         AND TRY_CAST(actual_dep_ts AS datetime2(0)) IS NULL
                         AND TRY_CAST(actual_arr_ts AS datetime2(0)) IS NULL
                    THEN 'Yes'
                    WHEN UPPER(TRIM(cancelled_flag)) IN ('FALSE','0','N') THEN 'No'
                    ELSE 'n/a'
                END AS cancelled_flag,
                CASE
                    WHEN UPPER(TRIM(diverted_flag)) IN ('TRUE','1','Y') THEN 'Yes'
                    WHEN UPPER(TRIM(diverted_flag)) IN ('FALSE','0','N') THEN 'No'
                    ELSE 'n/a'
                END AS diverted_flag,
                aircraft_id,
                status_text,
                TRY_CAST(last_updated_ts AS datetime2(0)) AS last_updated_ts,
                ROW_NUMBER() OVER (
                    PARTITION BY raw_flight_id 
                    ORDER BY TRY_CAST(last_updated_ts AS datetime2(0)) DESC
                ) AS rn
            FROM bronze.flights
        )
        INSERT INTO silver.flights (
            raw_flight_id,
            flight_number,
            flight_date,
            origin,
            destination,
            scheduled_dep_ts,
            actual_dep_ts,
            scheduled_arr_ts,
            actual_arr_ts,
            cancelled_flag,
            diverted_flag,
            aircraft_id,
            status_text,
            last_updated_ts
        )
        SELECT
            raw_flight_id,
            flight_number,
            flight_date,
            origin,
            destination,
            scheduled_dep_ts,
            actual_dep_ts,
            scheduled_arr_ts,
            actual_arr_ts,
            cancelled_flag,
            diverted_flag,
            aircraft_id,
            status_text,
            last_updated_ts
        FROM ranked
        WHERE rn = 1;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- =========================================================
        -- Loading silver.delay_events
        -- =========================================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.delay_events';
        TRUNCATE TABLE silver.delay_events;

        PRINT '>> Inserting Data Into: silver.delay_events';
        ;WITH ranked AS (
            SELECT
                delay_event_id,
                raw_flight_id,
                TRY_CAST(delay_ts AS datetime2(0)) AS delay_ts,
                CASE
                    WHEN UPPER(delay_category) IN ('MAINT','MX') THEN 'MAINT'
                    WHEN UPPER(delay_category) IN ('WX','WEATHER') THEN 'WEATHER'
                    WHEN UPPER(delay_category) IN ('CRW','CREW') THEN 'CREW'
                    ELSE UPPER(delay_category)
                END AS delay_category,
                NULLIF(UPPER(TRIM(delay_code)), '') AS delay_code,
                CASE 
                    WHEN TRY_CAST(delay_minutes AS INT) < 0 THEN 0
                    ELSE TRY_CAST(delay_minutes AS INT)
                END AS delay_minutes,
                CASE 
                    WHEN notes IS NULL OR LTRIM(RTRIM(notes)) = '' THEN 'unknown'
                    ELSE LOWER(TRIM(notes))
                END AS notes,
                ROW_NUMBER() OVER (
                    PARTITION BY delay_event_id
                    ORDER BY TRY_CAST(delay_ts AS datetime2(0)) DESC
                ) AS rn
            FROM bronze.delay_events
            )

            INSERT INTO silver.delay_events (
            delay_event_id,
            raw_flight_id,
            delay_ts,
            delay_category,
            delay_code,
            delay_minutes,
            notes
            )
            SELECT
            delay_event_id,
            raw_flight_id,
            delay_ts,
            delay_category,
            delay_code,
            delay_minutes,
            notes
            FROM ranked
            WHERE rn = 1;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- =========================================================
        -- Loading silver.weather_windows (valid ranges only)
        -- =========================================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.weather_windows';
        TRUNCATE TABLE silver.weather_windows;

        PRINT '>> Inserting Data Into: silver.weather_windows';
        INSERT INTO silver.weather_windows (
            weather_window_id,
            airport_code,
            weather_start_ts,
            weather_end_ts,
            weather_type,
            severity
        )
        SELECT
            weather_window_id,
            UPPER(LTRIM(RTRIM(airport_code))) AS airport_code,
            TRY_CAST(weather_start_ts AS datetime2(0)) AS weather_start_ts,
            TRY_CAST(weather_end_ts AS datetime2(0)) AS weather_end_ts,
            UPPER(LTRIM(RTRIM(weather_type))) AS weather_type,
            TRY_CAST(severity AS INT) AS severity
        FROM bronze.weather_windows
        WHERE TRY_CAST(weather_end_ts AS datetime2(0)) >= TRY_CAST(weather_start_ts AS datetime2(0));

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';



        -- =========================================================
        -- Batch Completion
        -- =========================================================
        SET @batch_end_time = GETDATE();
        PRINT '==========================================';
        PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '==========================================';

    END TRY
    BEGIN CATCH
        PRINT '==========================================';
        PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==========================================';
    END CATCH
END;
