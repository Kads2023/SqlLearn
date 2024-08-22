
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



-- =============================================
-- Author:      Kathambaree
-- ALTER Date:  2024-08-21
-- Description: Query fact instrument statistics
-- =============================================
CREATE PROCEDURE [MainWH].[sp_DWH_queryfactinstrumentstatistics]
	@InstrumentIdList varchar(max) = '',
	@YearsList varchar(max) = '',
	@MonthsList varchar(max) = ''
AS
BEGIN
	DECLARE @InstrumentIdJoin AS varchar(max) = '';
	DECLARE @YearsListJoin AS varchar(max) = '';
	DECLARE @MonthsListJoin AS varchar(max) = '';
	DECLARE @ExecSQL AS varchar(max) = '';
	DECLARE @InsertMonths AS varchar(max) = '';
	
	
	IF NULLIF(@InstrumentIdList, '') IS NULL
		BEGIN
			SET @InstrumentIdJoin = ''
		END
	ELSE
		BEGIN
			SET @InstrumentIdJoin = 'JOIN #InstrumentIds input_ids 
									 ON fis.InstrumentId COLLATE SQL_Latin1_General_CP1_CI_AS = input_ids.InstrumentId COLLATE SQL_Latin1_General_CP1_CI_AS'
		END

	
	IF NULLIF(@MonthsList, '') IS NULL
		BEGIN
			SET @MonthsListJoin = ''
		END
	ELSE
		BEGIN
			SET @MonthsListJoin = '
				JOIN #Years
				ON 
				(fis.YearFrom BETWEEN min_year AND max_year
				--OR
				--fis.YearTo BETWEEN min_year AND max_year
				)
				JOIN #Months
				ON
				(fis.MonthFrom BETWEEN min_month AND max_month
				--OR
				--fis.MonthTo BETWEEN min_month AND max_month
				)'
		END

	IF NULLIF(@YearsList, '') IS NULL 
		IF NULLIF(@YearsListJoin, '') IS NULL
		BEGIN
			SET @YearsListJoin = ''
		END
		ELSE
			BEGIN
				SET @YearsListJoin = '
				JOIN #Years
				ON 
				(fis.YearFrom BETWEEN min_year AND max_year
				--OR
				--fis.YearTo BETWEEN min_year AND max_year
				)'
			END;
	
	SET @ExecSQL = 'SELECT
			fis.*
			FROM
			[SDPWH].[MainWH].[factinstrumentstatistics] fis
		 	' + @InstrumentIdJoin + @YearsListJoin + @MonthsListJoin + '
			ORDER BY InstrumentId, YearFrom, MonthFrom, YearTo, MonthTo, Type';

	CREATE OR REPLACE TABLE #InstrumentIds 
	(
	   InstrumentId [varchar](8000) NULL
	);

	INSERT INTO #InstrumentIds SELECT value AS InstrumentId FROM STRING_SPLIT(@InstrumentIdList, ',');
	
	CREATE TABLE #AllYears 
	(
	   InYear varchar(4) NULL
	);
		
	INSERT INTO #AllYears SELECT value AS InYear FROM STRING_SPLIT(@YearsList, ',');
	
	CREATE TABLE #AllYearMonths 
	(
	   InYear varchar(4) NULL,
	   InMonth varchar(2) NULL
	);
		
	INSERT INTO #AllYearMonths SELECT YEAR(in_date) AS InYear, MONTH(in_date) AS InMonth
		FROM
		(
			SELECT
				CASE 
					WHEN year_month is not NULL AND year_month != ''
					THEN cast(year_month + '-01' as DATE)
					ELSE cast(NULL as DATE)
				END AS in_date
			FROM
			(
				SELECT value as year_month
				FROM 
				STRING_SPLIT(@MonthsList, ',')
			) in_months
		) in_months_fin;


	CREATE TABLE #Years 
	(
	   min_year varchar(4) NULL,
	   max_year varchar(4) NULL
	);
	
	
	WITH
	all_years as (
		SELECT 
			InYear 
		FROM 
		#AllYears
		UNION
		SELECT 
			InYear 
		FROM 
		#AllYearMonths
	)
	
	INSERT INTO ##Years SELECT min(InYear) as min_year, max(InYear) as max_year FROM all_years;

	
	CREATE TABLE ##Months
	(
	   min_month varchar(2) NULL,
	   max_month varchar(2) NULL
	);

	INSERT INTO #Months SELECT min(InMonth) as min_month, max(InMonth) as max_month FROM #AllYearMonths;

PRINT @ExecSQL;

execute (@ExecSQL);

END;
GO

