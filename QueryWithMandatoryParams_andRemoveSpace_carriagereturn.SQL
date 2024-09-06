/****** Object:  StoredProcedure [MainWH].[sp_DWH_queryfactinstrumentstatistics]    Script Date: 05/09/2024 14:12:26 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





-- =============================================
-- Author:      Kathambaree
-- ALTER Date:  2024-08-21
-- Description: Query fact instrument statistics
-- =============================================
ALTER PROCEDURE [SalesWH].[sp_DWH_queryfactinstrumentstatistics]
	@BBGIdList varchar(max) = '',
	@YearsList varchar(max) = '',
	@MonthsList varchar(max) = '',
	@SourceEnv AS nvarchar(max) = 'sales'
AS
BEGIN
	DECLARE @BBGIdJoin AS nvarchar(max) = NULL;
	DECLARE @YearsListJoin AS nvarchar(max) = NULL;
	DECLARE @MonthsListJoin AS nvarchar(max) = NULL;
	DECLARE @ExecSQL AS nvarchar(max) = NULL;
	DECLARE @WithClause AS nvarchar(max) = NULL;
	DECLARE @SelectClause AS nvarchar(max) = NULL;
	DECLARE @OrderByClause AS nvarchar(max) = NULL;
	DECLARE @GoldDataBaseName AS nvarchar(max) = NULL;
	DECLARE @GoldTableName AS nvarchar(max) = NULL;
	DECLARE @StripString varchar(80) = 'STUFF(value,1,ISNULL(NULLIF(PATINDEX(@Pattern,value),0)-1,0),'''')';
	DECLARE @PatternStart varchar(5) = '%[^';
	DECLARE @PatternEnd varchar(5) = ']%';
	IF NULLIF(@SourceEnv, '') IS NULL
		BEGIN
			SET @GoldDataBaseName = 'MainWH'
			SET @GoldTableName = 'factinstrumentstatistics'
		END
	ELSE
		BEGIN
			SET @GoldDataBaseName = '' + UPPER(LEFT(@SourceEnv,1))+LOWER(SUBSTRING(@SourceEnv,2,LEN(@SourceEnv))) + 'WH'
			SET @GoldTableName = 'factinstrumentstatistics'
		END
		
	IF NULLIF(@BBGIdList, '') IS NULL
		BEGIN
			RAISERROR('BBGID is mandatory', 16,1)
		END
	ELSE
		BEGIN
			PRINT '@GoldDataBaseName';
			PRINT @GoldDataBaseName;
	
			PRINT '@GoldTableName';
			PRINT @GoldTableName;
	
			SET @BBGIdJoin = 'JOIN input_ids 
			ON fis.CompositeBbgid COLLATE SQL_Latin1_General_CP1_CI_AS = TRIM(CHAR(9) + CHAR(10) + CHAR(13) + CHAR(32) FROM input_ids.CompositeBbgid) COLLATE SQL_Latin1_General_CP1_CI_AS'

			PRINT '@BBGIdJoin';
			PRINT @BBGIdJoin;
	
			IF NULLIF(@MonthsList, '') IS NULL
				BEGIN
					SET @MonthsListJoin = ''
					IF NOT NULLIF(@YearsList, '') IS NULL
						BEGIN
							SET @YearsListJoin = '
							JOIN input_min_max_years 
							ON 
							(	fis.YearFrom BETWEEN min_year AND max_year
								AND fis.YearTo BETWEEN min_year AND max_year
							)'
						END
					ELSE
						BEGIN
							SET @YearsListJoin = ''
						END
				END
			ELSE
				BEGIN
					SET @YearsListJoin = '
					JOIN input_min_max_years 
					ON 
					(	fis.YearFrom BETWEEN min_year AND max_year
						AND fis.YearTo BETWEEN min_year AND max_year
					)'
					SET @MonthsListJoin = '
					JOIN input_min_max_months 
					ON 
					(
						fis.MonthFrom BETWEEN min_month AND max_month
						AND fis.MonthTo BETWEEN min_month AND max_month
					)'
				END
		
			PRINT '@MonthsListJoin';
			PRINT @MonthsListJoin;
	
			PRINT '@YearsListJoin';
			PRINT @YearsListJoin;

			SET @WithClause = '
			DECLARE @Pattern varchar(15) = ''' + @PatternStart + ''' + Char(9) + CHAR(10) + CHAR(13) + CHAR(32) + ''' + @PatternEnd + ''';
			WITH input_ids as (
				SELECT ' + @StripString + ' AS CompositeBbgid FROM STRING_SPLIT( ''' + @BBGIdList + ''', '','')
			),
			input_years as (
				SELECT YEAR(in_date) as Years
				FROM
				(
					SELECT
						CASE 
							WHEN NULLIF(Years, '''') is not NULL
							THEN cast(TRIM(CHAR(9) + CHAR(10) + CHAR(13) + CHAR(32) FROM Years) + ''-01-01'' as DATE)
							ELSE cast(NULL as DATE)
						END AS in_date
					FROM
					(
						SELECT ' + @StripString + ' AS Years FROM STRING_SPLIT(''' + @YearsList + ''', '','')
					) in_years
				) in_years_fin
			),
			input_months as (
				SELECT YEAR(in_date) as Years,MONTH(in_date) AS Months
				FROM
				(
					SELECT
						CASE 
							WHEN NULLIF(year_month, '''') is not NULL
							THEN cast(TRIM(CHAR(9) + CHAR(10) + CHAR(13) + CHAR(32) FROM year_month) + ''-01'' as DATE)
							ELSE cast(NULL as DATE)
						END AS in_date
					FROM
					(
						SELECT ' + @StripString + ' as year_month FROM STRING_SPLIT(''' + @MonthsList + ''', '','')
					) in_months
				) in_months_fin
			),
			all_years as (
				SELECT Years FROM input_years
				UNION
				SELECT Years FROM input_months
			),
			input_min_max_years as (
				SELECT min(Years) as min_year, max(Years) as max_year FROM all_years
			),
			input_min_max_months as (
				SELECT min(Months) as min_month, max(Months) as max_month FROM input_months
			)
			';
	
			--PRINT '@WithClause';
			--PRINT @WithClause;

			SET @SelectClause = 'SELECT
			fis.*
			FROM
			[SDPWH].[' + @GoldDataBaseName + '].[' + @GoldTableName + '] fis';
	
			--PRINT '@SelectClause';
			--PRINT @SelectClause;

			SET @OrderByClause = '
			ORDER BY CompositeBbgid, YearFrom, MonthFrom, YearTo, MonthTo, Type';
	
			--PRINT '';
			--PRINT '@OrderByClause';
			--PRINT @OrderByClause;
	
			SET @ExecSQL = '' + @WithClause + '' + @SelectClause + ' ' + @BBGIdJoin + ' ' + @YearsListJoin + ' ' + @MonthsListJoin + '' + @OrderByClause + '';

			PRINT '';
			PRINT '@ExecSQL';
			PRINT @ExecSQL;
			PRINT '';

			EXEC sp_executesql @ExecSQL;
		END
END;
GO


