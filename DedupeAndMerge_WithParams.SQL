SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


-- =============================================
-- Author:      Kathambaree
-- ALTER Date:  2024-08-07
-- Description: Snp usdexchangerates
-- =============================================
ALTER PROCEDURE [MainWH].[sp_DWH_snpexchangerate]
	@SourceEnv AS nvarchar(max) = NULL
AS
BEGIN
	DECLARE @FinalTableName AS nvarchar(max) = NULL;

	IF NULLIF(@SourceEnv, '') IS NULL
		BEGIN
			SET @FinalTableName = 'snpexchangerate'
		END
	ELSE
		BEGIN
			SET @FinalTableName = LOWER(@SourceEnv) + '_snpexchangerate'
		END

	PRINT '@FinalTableName';
	PRINT @FinalTableName;
	
	DECLARE @QueryToExecute AS nvarchar(max) = NULL;

	SET @QueryToExecute = '
	
		-- This table hold numbers of loaded rows --
	UPDATE SDPWH.MainWH.MNGDataLoaded
	SET IndLastUpdate = 0
	WHERE Layer = ''Gold''
		AND TableName = ''MainWH.'+@FinalTableName+'''
		AND IndLastUpdate = 1;

	DECLARE @TransactionActive BIT = 0;
	IF @@TRANCOUNT = 0
	BEGIN
		BEGIN TRANSACTION;
		SET @TransactionActive = 1;
	END

	BEGIN TRY
		DECLARE @UpdateRowCount AS BIGINT;
		DECLARE @InsertRowCount AS BIGINT;
		DECLARE @MaxCurrecnyExchangeRateKey AS BIGINT;

		SELECT @MaxCurrecnyExchangeRateKey = ISNULL(MAX(CurrecnyExchangeRateKey), 0)
		FROM [MainWH].['+@FinalTableName+'];

		
		-- UPDATE existing records	
		UPDATE TARGET
		SET TARGET.IssueDateKey = SOURCE.IssueDateKey,
			TARGET.IssueDate = SOURCE.IssueDate,
			TARGET.CreatedDate = SOURCE.CreatedDate,
			TARGET.CurrencyCode = SOURCE.CurrencyCode,
			TARGET.Id = SOURCE.Id,
			TARGET.ExchangeRate = SOURCE.ExchangeRate,
			TARGET.DWHUpdated = GETUTCDATE()
		FROM [MainWH].['+@FinalTableName+'] AS TARGET
		JOIN [SilverLakeHouse].[dbo].['+@FinalTableName+'] AS SOURCE
			ON TARGET.CurrencyCode = SOURCE.CurrencyCode
				AND TARGET.CreatedDate <= SOURCE.CreatedDate
				AND TARGET.IssueDate = SOURCE.IssueDate
				AND (
					ISNULL(TARGET.Id, '''') <> ISNULL(SOURCE.Id, '''')					
					OR ISNULL(TARGET.CurrencyCode, '''') <> ISNULL(SOURCE.CurrencyCode, '''')
					OR ISNULL(TARGET.CreatedDate, ''1900-01-01'') <> ISNULL(SOURCE.CreatedDate, ''1900-01-01'')
					OR ISNULL(TARGET.IssueDate, ''1900-01-01'') <> ISNULL(SOURCE.IssueDate, ''1900-01-01'')
					)

		SET @UpdateRowCount = @@ROWCOUNT;

		-- INSERT new records
		INSERT INTO [MainWH].['+@FinalTableName+'] (
		CurrecnyExchangeRateKey,
		IssueDate,
		IssueDateKey,
		CreatedDate,
		CurrencyCode,
		Id,
		ExchangeRate,
		DWHCreated,
		DWHUpdated
		)
		SELECT @MaxCurrecnyExchangeRateKey + ROW_NUMBER() OVER (
			ORDER BY (
					SELECT NULL
					)
			),
		IssueDate,
		IssueDateKey,
		CreatedDate,
		CurrencyCode,
		Id,
		ExchangeRate,
		GETUTCDATE(),
		GETUTCDATE()
		FROM [SilverLakeHouse].[dbo].['+@FinalTableName+'] AS SOURCE
		WHERE NOT EXISTS (
			SELECT 1
			FROM [MainWH].['+@FinalTableName+'] AS TARGET
			WHERE TARGET.CurrencyCode = SOURCE.CurrencyCode
				AND TARGET.CreatedDate <= SOURCE.CreatedDate
				AND TARGET.IssueDate = SOURCE.IssueDate
			);

		SET @InsertRowCount = @@ROWCOUNT

		-- Add new record of how many row were updated/inserted for this session --
		INSERT INTO SDPWH.MainWH.MNGDataLoaded
		VALUES (
			''Gold'',
			''MainWH.'+@FinalTableName+''',
			GETUTCDATE(),
			@UpdateRowCount,
			@InsertRowCount,
			1,
			0,
			NULL,
			CAST(FORMAT(GETUTCDATE(),''yyyyMMdd'') AS INT),
			CAST(FORMAT(GETUTCDATE(),''HHmmss'') AS INT)
			)

		COMMIT TRANSACTION;
		
	END TRY

	BEGIN CATCH
		IF @TransactionActive = 1 AND @@TRANCOUNT > 0
		BEGIN
			ROLLBACK TRANSACTION;
			SET @TransactionActive = 0;
		END

        DECLARE @ErrorMessage NVARCHAR(500);
		
		SET @ErrorMessage = ERROR_MESSAGE()

        PRINT ''Error occurred: '' + @ErrorMessage;

		INSERT INTO SDPWH.MainWH.MNGDataLoaded
		VALUES (
			''Gold'',
			''MainWH.'+@FinalTableName+''',
			GETUTCDATE(),
			0,
			0,
			1,
			1,
			@ErrorMessage,
			CAST(FORMAT(GETUTCDATE(),''yyyyMMdd'') AS INT),
			CAST(FORMAT(GETUTCDATE(),''HHmmss'') AS INT)
			);

        THROW;

	END CATCH;
		'
		
	PRINT '@QueryToExecute';
	PRINT @QueryToExecute;

	EXEC sp_executesql @QueryToExecute;
	
END;
GO


