/****** Object:  StoredProcedure [MainWH].[sp_DWH_factinstrumentstatistics]    Script Date: 22/08/2024 18:04:10 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





-- =============================================
-- Author:      Kathambaree
-- ALTER Date:  2024-08-20
-- Description: fact instrument statistics
-- =============================================
CREATE PROCEDURE [MainWH].[sp_DWH_factinstrumentstatistics]
AS
BEGIN
	DECLARE @UpdateRowCount AS BIGINT;
	DECLARE @InsertRowCount AS BIGINT;
	DECLARE @MaxInstrumentStatisticsKey AS BIGINT;

	SELECT @MaxInstrumentStatisticsKey = ISNULL(MAX(InstrumentStatisticsKey), 0)
	FROM [MainWH].[factinstrumentstatistics];

		-- This table hold numbers of loaded rows --
	UPDATE SDPWH.MainWH.MNGDataLoaded
	SET IndLastUpdate = 0
	WHERE Layer = 'Gold'
		AND TableName = 'MainWH.factinstrumentstatistics'
		AND IndLastUpdate = 1


	DECLARE @TransactionActive BIT = 0;
	IF @@TRANCOUNT = 0
	BEGIN
		BEGIN TRANSACTION;
		SET @TransactionActive = 1;
	END

		
	BEGIN TRY

		-- UPDATE existing records	
		UPDATE TARGET
		SET TARGET.InstrumentId = SOURCE.InstrumentId,
			TARGET.InstrumentKey = SOURCE.InstrumentKey,
			TARGET.YearFrom = SOURCE.YearFrom,
			TARGET.MonthFrom = SOURCE.MonthFrom,
			TARGET.YearTo = SOURCE.YearTo,
			TARGET.MonthTo = SOURCE.MonthTo,
			TARGET.Type = SOURCE.Type,
			TARGET.Currency = SOURCE.Currency,
			TARGET.Value = SOURCE.Value,
			TARGET.DWHUpdated = GETUTCDATE()
		FROM [MainWH].[factinstrumentstatistics] AS TARGET
		JOIN [SilverLakeHouse].[dbo].[factinstrumentstatistics] AS SOURCE
			ON TARGET.InstrumentId = SOURCE.InstrumentId
				AND TARGET.InstrumentKey = SOURCE.InstrumentKey
				AND TARGET.YearFrom = SOURCE.YearFrom
				AND TARGET.MonthFrom = SOURCE.MonthFrom
				AND TARGET.YearTo = SOURCE.YearTo
				AND TARGET.MonthTo = SOURCE.MonthTo
				AND TARGET.Type = SOURCE.Type
				AND 
				(
					TARGET.Currency = SOURCE.Currency
					OR ISNULL(TARGET.Currency, '') = ISNULL(SOURCE.Currency, '')
				);

		SET @UpdateRowCount = @@ROWCOUNT;

		-- INSERT new records
		INSERT INTO [MainWH].[factinstrumentstatistics] (
		InstrumentStatisticsKey,
		InstrumentId,
		InstrumentKey,
		YearFrom,
		MonthFrom,
		YearTo,
		MonthTo,
		Type,
		Currency,
		Value,
		DWHCreated,
		DWHUpdated
		)
		SELECT @MaxInstrumentStatisticsKey + ROW_NUMBER() OVER (
			ORDER BY (
					SELECT NULL
					)
			),
		InstrumentId,
		InstrumentKey,
		YearFrom,
		MonthFrom,
		YearTo,
		MonthTo,
		Type,
		Currency,
		Value,
		GETUTCDATE(),
		GETUTCDATE()
		FROM [SilverLakeHouse].[dbo].[factinstrumentstatistics] AS SOURCE
		WHERE NOT EXISTS (
			SELECT 1
			FROM [MainWH].[factinstrumentstatistics] AS TARGET
			WHERE TARGET.InstrumentId = SOURCE.InstrumentId
				AND TARGET.InstrumentKey = SOURCE.InstrumentKey
				AND TARGET.YearFrom = SOURCE.YearFrom
				AND TARGET.MonthFrom = SOURCE.MonthFrom
				AND TARGET.YearTo = SOURCE.YearTo
				AND TARGET.MonthTo = SOURCE.MonthTo
				AND TARGET.Type = SOURCE.Type
				AND 
				(
					TARGET.Currency = SOURCE.Currency
					OR ISNULL(TARGET.Currency, '') = ISNULL(SOURCE.Currency, '')
				)
			);

		SET @InsertRowCount = @@ROWCOUNT

		-- Add new record of how many row were updated/inserted for this session --
		INSERT INTO SDPWH.MainWH.MNGDataLoaded
		VALUES (
			'Gold',
			'MainWH.factinstrumentstatistics',
			GETUTCDATE(),
			@UpdateRowCount,
			@InsertRowCount,
			1,
			0,
			NULL,
			CAST(FORMAT(GETUTCDATE(),'yyyyMMdd') AS INT),
			CAST(FORMAT(GETUTCDATE(),'HHmmss') AS INT)
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

        PRINT 'Error occurred: ' + @ErrorMessage;

		INSERT INTO SDPWH.MainWH.MNGDataLoaded
		VALUES (
			'Gold',
			'MainWH.factinstrumentstatistics',
			GETUTCDATE(),
			0,
			0,
			1,
			1,
			@ErrorMessage,
			CAST(FORMAT(GETUTCDATE(),'yyyyMMdd') AS INT),
			CAST(FORMAT(GETUTCDATE(),'HHmmss') AS INT)
			);

        THROW;

	END CATCH
END;
GO


