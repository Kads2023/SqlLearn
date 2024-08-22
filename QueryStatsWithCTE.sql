				
	

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



-- =============================================
-- Author:      Kathambaree
-- ALTER Date:  2024-08-21
-- Description: Query fact instrument statistics
-- =============================================
ALTER PROCEDURE [MainWH].[sp_DWH_queryfactinstrumentstatistics]
	@InstrumentIdList varchar(max),
	@YearsList varchar(max),
	@MonthsList varchar(max)
AS
BEGIN
	DECLARE @InstrumentIdJoin AS nvarchar(max) = NULL;
	DECLARE @YearsListJoin AS nvarchar(max) = NULL;
	DECLARE @MonthsListJoin AS nvarchar(max) = NULL;
	DECLARE @ExecSQL AS nvarchar(max) = NULL;
	DECLARE @WithClause AS nvarchar(max) = NULL;
	DECLARE @SelectClause AS nvarchar(max) = NULL;
	DECLARE @OrderByClause AS nvarchar(max) = NULL;

	IF NULLIF(@InstrumentIdList, '') IS NULL
		BEGIN
			SET @InstrumentIdJoin = ''
		END
	ELSE
		BEGIN
			SET @InstrumentIdJoin = 'JOIN input_ids 
			ON fis.InstrumentId COLLATE SQL_Latin1_General_CP1_CI_AS = input_ids.InstrumentId COLLATE SQL_Latin1_General_CP1_CI_AS'
		END

	PRINT '@InstrumentIdJoin';
	PRINT @InstrumentIdJoin;
	
	IF NULLIF(@MonthsList, '') IS NULL
		BEGIN
			SET @MonthsListJoin = ''
			IF NOT NULLIF(@YearsList, '') IS NULL
				BEGIN
					SET @YearsListJoin = 'JOIN input_min_max_years ON fis.YearFrom BETWEEN min_year AND max_year'
				END
			ELSE
				BEGIN
					SET @YearsListJoin = ''
				END
		END
	ELSE
		BEGIN
			SET @YearsListJoin = 'JOIN input_min_max_years ON fis.YearFrom BETWEEN min_year AND max_year'
			SET @MonthsListJoin = 'JOIN input_min_max_months ON fis.MonthFrom BETWEEN min_month AND max_month'
		END
		
	PRINT '@MonthsListJoin';
	PRINT @MonthsListJoin;
	
	PRINT '@YearsListJoin';
	PRINT @YearsListJoin;

	SET @WithClause = 'WITH input_ids as (
		SELECT value AS InstrumentId FROM STRING_SPLIT( ''' + @InstrumentIdList + ''', '','')
	),
	input_years as (
		SELECT value AS Years FROM STRING_SPLIT(''' + @YearsList + ''', '','')
	),
	input_months as (
		SELECT YEAR(in_date) as Years,MONTH(in_date) AS Months
		FROM
		(
			SELECT
				CASE 
					WHEN NULLIF(year_month, '''') is not NULL
					THEN cast(year_month + ''-01'' as DATE)
					ELSE cast(NULL as DATE)
				END AS in_date
			FROM
			(
				SELECT value as year_month FROM STRING_SPLIT(''' + @MonthsList + ''', '','')
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
	[SDPWH].[MainWH].[factinstrumentstatistics] fis';
	
	--PRINT '@SelectClause';
	--PRINT @SelectClause;

	SET @OrderByClause = '
	ORDER BY InstrumentId, YearFrom, MonthFrom, YearTo, MonthTo, Type';
	
	--PRINT '';
	--PRINT '@OrderByClause';
	--PRINT @OrderByClause;
	
	SET @ExecSQL = '' + @WithClause + '' + @SelectClause + ' ' + @InstrumentIdJoin + ' ' + @YearsListJoin + ' ' + @MonthsListJoin + '' + @OrderByClause + '';

	PRINT '';
	PRINT '@ExecSQL';
	PRINT @ExecSQL;
	PRINT '';

	EXEC sp_executesql @ExecSQL;

END;
GO
