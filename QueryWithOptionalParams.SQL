SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




-- =============================================
-- Author:      Kathambaree
-- ALTER Date:  2024-08-21
-- Description: Query fact instrument statistics
-- =============================================
ALTER PROCEDURE [MainWH].[sp_DWH_queryfactreratereccomendations]
	@TopNValue varchar(max) = '',
	@BorrowersList varchar(max) = '',
	@LendersList varchar(max) = '',
	@ISINsList varchar(max) = '',
	@TickersList varchar(max) = '',
	@SourceEnv AS nvarchar(max) = 'sales'
AS
BEGIN
	DECLARE @DefaultTopNValue varchar(max) = '30';
	DECLARE @DefaultTopNOrderBy varchar(max) = 'PercentDeltaSGFto7VWAF DESC';
	DECLARE @LoanIdJoin AS nvarchar(max) = NULL;
	DECLARE @BorrowersListJoin AS nvarchar(max) = NULL;
	DECLARE @LendersListJoin AS nvarchar(max) = NULL;
	DECLARE @ISINsListJoin AS nvarchar(max) = NULL;
	DECLARE @TickersListJoin AS nvarchar(max) = NULL;
	DECLARE @LoanIdSelect AS nvarchar(max) = NULL;
	DECLARE @ExecSQL AS nvarchar(max) = NULL;
	DECLARE @WithClause AS nvarchar(max) = NULL;
	DECLARE @SelectClause AS nvarchar(max) = NULL;
	DECLARE @OrderByClause AS nvarchar(max) = NULL;
	DECLARE @FinalTablePrefix AS nvarchar(max) = NULL;
	
	IF NULLIF(@SourceEnv, '') IS NULL
		BEGIN
			SET @FinalTablePrefix = ''
		END
	ELSE
		BEGIN
			SET @FinalTablePrefix = LOWER(@SourceEnv) + '_'
		END


	IF NULLIF(@TopNValue, '') IS NULL
		BEGIN
			SET @LoanIdSelect = 'SELECT TOP ' + @DefaultTopNValue + ' LoanId FROM [MainWH].[' + @FinalTablePrefix + 'factreratereccomendations] ORDER BY ' + @DefaultTopNOrderBy
		END
	ELSE
		BEGIN
			SET @LoanIdSelect = 'SELECT TOP ' + @TopNValue + ' LoanId FROM [MainWH].[' + @FinalTablePrefix + 'factreratereccomendations] ORDER BY ' + @DefaultTopNOrderBy + ' DESC '
		END
		
	PRINT '@LoanIdSelect';
	PRINT @LoanIdSelect;
	
	SET @LoanIdJoin = '
		JOIN top_n_ids 
		ON frr.LoanId COLLATE SQL_Latin1_General_CP1_CI_AS = top_n_ids.LoanId COLLATE SQL_Latin1_General_CP1_CI_AS';

	PRINT '@LoanIdJoin';
	PRINT @LoanIdJoin;
	
	
	IF NULLIF(@BorrowersList, '') IS NULL
		BEGIN
			SET @BorrowersListJoin = '';
		END
	ELSE
		BEGIN
			SET @BorrowersListJoin = 'JOIN input_borrowers 
			ON frr.BorrowerKey COLLATE SQL_Latin1_General_CP1_CI_AS = input_borrowers.BorrowerKey COLLATE SQL_Latin1_General_CP1_CI_AS';
		END

	PRINT '@BorrowersListJoin';
	PRINT @BorrowersListJoin;
	
	IF NULLIF(@LendersList, '') IS NULL
		BEGIN
			SET @LendersListJoin = '';
		END
	ELSE
		BEGIN
			SET @LendersListJoin = 'JOIN input_lenders
			ON frr.LenderKey COLLATE SQL_Latin1_General_CP1_CI_AS = input_lenders.LenderKey COLLATE SQL_Latin1_General_CP1_CI_AS';
		END

	PRINT '@LendersListJoin';
	PRINT @LendersListJoin;
	
	IF NULLIF(@ISINsList, '') IS NULL
		BEGIN
			SET @ISINsListJoin = '';
		END
	ELSE
		BEGIN
			SET @ISINsListJoin = 'JOIN input_isins
			ON frr.InstrumentId COLLATE SQL_Latin1_General_CP1_CI_AS = input_isins.InstrumentId COLLATE SQL_Latin1_General_CP1_CI_AS';
		END

	PRINT '@ISINsListJoin';
	PRINT @ISINsListJoin;
	
	IF NULLIF(@TickersList, '') IS NULL
		BEGIN
			SET @TickersListJoin = '';
		END
	ELSE
		BEGIN
			SET @TickersListJoin = 'JOIN input_tickers
			ON frr.InstrumentId COLLATE SQL_Latin1_General_CP1_CI_AS = input_tickers.InstrumentId COLLATE SQL_Latin1_General_CP1_CI_AS';
		END

	PRINT '@TickersListJoin';
	PRINT @TickersListJoin;
	
	SET @WithClause = 'WITH top_n_ids as (
		SELECT DISTINCT LoanId FROM ( ' + @LoanIdSelect + ' ) topn
	),
	input_borrowers as (
		SELECT 
			BorrowerKey
		FROM 
			[MainWH].[' + @FinalTablePrefix + 'dimborrowers]
		WHERE
			BorrowerCode IN ( SELECT value AS BorrowerCode FROM STRING_SPLIT(''' + @BorrowersList + ''', '','')	)
	),
	input_lenders as (
		SELECT 
			NonMinorLenderKey AS LenderKey
		FROM 
			[MainWH].[' + @FinalTablePrefix + 'dimlenders]
		WHERE
			NonMinorLenderCode IN ( SELECT value AS NonMinorLenderCode FROM STRING_SPLIT(''' + @LendersList + ''', '','')	)
	),
	input_isins as (
		SELECT 
			InstrumentId
		FROM 
			[MainWH].[' + @FinalTablePrefix + 'diminstruments]
		WHERE
			Isin IN ( SELECT value AS Isin FROM STRING_SPLIT(''' + @ISINsList + ''', '','')	)
	),
	input_tickers as (
		SELECT 
			InstrumentId
		FROM 
			[MainWH].[' + @FinalTablePrefix + 'diminstruments]
		WHERE
			CompositeTicker IN ( SELECT value AS CompositeTicker FROM STRING_SPLIT(''' + @TickersList + ''', '','')	)
	)';
	
	PRINT '@WithClause';
	PRINT @WithClause;

	SET @SelectClause = '
	SELECT
	frr.*
	FROM
	[SDPWH].[MainWH].[' + @FinalTablePrefix + 'factreratereccomendations] frr';
	
	PRINT '@SelectClause';
	PRINT @SelectClause;

	SET @OrderByClause = '
	ORDER BY LoanId, RecommendationDate,' + @DefaultTopNOrderBy;
	
	PRINT '';
	PRINT '@OrderByClause';
	PRINT @OrderByClause;
	
	SET @ExecSQL = '' + @WithClause + '' + @SelectClause + ' ' + @LoanIdJoin + ' ' + @BorrowersListJoin + ' ' + @LendersListJoin + ' ' + @OrderByClause + ' ';

	PRINT '';
	PRINT '@ExecSQL';
	PRINT @ExecSQL;
	PRINT '';

	--EXEC sp_executesql @ExecSQL;

END;
GO


