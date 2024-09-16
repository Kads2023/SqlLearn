/****** Object:  StoredProcedure [ServiceWH].[sp_MNG_TriggerMNGSPGatewayRun]    Script Date: 16/09/2024 11:28:59 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




ALTER PROCEDURE  [ServiceWH].[sp_MNG_TriggerMNGSPGatewayRun]
    @ActionId INT,
	@CallerId   [varchar](800)=NULL,
    @Param1  VARCHAR(MAX) = NULL,
    @Param2  VARCHAR(MAX) = NULL,
    @Param3  VARCHAR(MAX) = NULL,
    @Param4  VARCHAR(MAX) = NULL,
    @Param5  VARCHAR(MAX) = NULL,
    @Param6  VARCHAR(MAX) = NULL,
    @Param7  VARCHAR(MAX) = NULL,
    @Param8  VARCHAR(MAX) = NULL,
    @Param9  VARCHAR(MAX) = NULL,
    @Param10 VARCHAR(MAX) = NULL,
    @Param11 VARCHAR(MAX) = NULL,
    @Param12 VARCHAR(MAX) = NULL,
    @Param13 VARCHAR(MAX) = NULL,
    @Param14 VARCHAR(MAX) = NULL,
    @Param15 VARCHAR(MAX) = NULL,
	@Param16 VARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;

	DECLARE @StartTime DATETIME2(3) = GETUTCDATE();
    DECLARE @EndTime DATETIME2(3);
    DECLARE @RowCount INT;
    DECLARE @Status VARCHAR(50) = 'Triggered';
    DECLARE @RunID VARCHAR(100) = (SELECT NEWID())
	


--	declare @actionid INT = 2
--	declare @CallerId   [varchar](400)  ='asaf'
--	 declare @param1 NVARCHAR(MAX) = 'BBG00NNJM2H5
--,BBG00M6PPWH3
--,BBG0010JZ1W9
--,BBG01H3QMQF2'
--	 declare @param2 NVARCHAR(MAX) = '2023,2024'
--	 declare @param3 NVARCHAR(MAX) = '2023-01,2024-12'
--	 declare @param4 NVARCHAR(MAX) = NULL
--	 declare @param5 NVARCHAR(MAX) = NULL
--	 declare @param6 NVARCHAR(MAX) = NULL
--	 declare @param7 NVARCHAR(MAX) = NULL
--	 declare @param8 NVARCHAR(MAX) = NULL
--	 declare @param9 NVARCHAR(MAX) = NULL
--	 declare @param10 NVARCHAR(MAX) = NULL
--	 declare @param11 NVARCHAR(MAX) = NULL
--	 declare @param12 NVARCHAR(MAX) = NULL
--	 declare @param13 NVARCHAR(MAX) = NULL
--	 declare @param14 NVARCHAR(MAX) = NULL
--	 declare @param15 NVARCHAR(MAX) = NULL
	



DECLARE @ParamList VARCHAR(MAX)=
(
SELECT CONCAT(
'ActionId:' , ISNULL(@ActionId , 'NULL') ,'  ||  ' ,
'@param1: ' , ISNULL(@Param1 , 'NULL') ,'  ||  ' ,
'@param2: ' , ISNULL(@Param2 , 'NULL') ,'  ||  ' ,
'@param3: ' , ISNULL(@Param3 , 'NULL') ,'  ||  ' ,
'@param4: ' , ISNULL(@Param4 , 'NULL') ,'  ||  ' ,
'@param5: ' , ISNULL(@Param5 , 'NULL') ,'  ||  ' ,
'@param6: ' , ISNULL(@Param6 , 'NULL') ,'  ||  ' ,
'@param7: ' , ISNULL(@Param7 , 'NULL') ,'  ||  ' ,
'@param8: ' , ISNULL(@Param8 , 'NULL') ,'  ||  ' ,
'@param9: ' , ISNULL(@Param9 , 'NULL') ,'  ||  ' ,
'@param10: ' ,ISNULL(@Param10, 'NULL') ,'  ||  ',
'@param11: ' ,ISNULL(@Param11, 'NULL') ,'  ||  ',
'@param12: ' ,ISNULL(@Param12, 'NULL') ,'  ||  ',
'@param13: ' ,ISNULL(@Param13, 'NULL') ,'  ||  ',
'@param14: ' ,ISNULL(@Param14, 'NULL') ,'  ||  ', 
'@param15: ' ,ISNULL(@Param15, 'NULL') ,'  ||  ', 
'@param16: ' ,ISNULL(@Param16, 'NULL') )
)
	

	set @ParamList = left(@ParamList,8000)    -- we are inserting to a column which is varchar(8000)

Insert into   [ServiceWH].[MNGSPGatewayRunLog]
	(
	 [RunId]
	,[Caller]   
	,[ActionId]
	,[Timestamp] 
	,[Status]  
	,[RunParameters]
	 
) 

VALUES (
     @RunID
	 ,@CallerId       --THE USER
	 ,@ActionId      --REQUESTED REPORT
	 ,GETUTCDATE()
	 ,@Status                --1 = RUNNING
	 ,@ParamList

)
	 
	 	 
	   BEGIN TRY

	   IF @ActionId IS NULL
		BEGIN
			RAISERROR('ActionId is mandatory', 16,1)
		END
	
	   	  
    DECLARE @procName NVARCHAR(128);
    DECLARE @numberOfParams INT;
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @paramValues NVARCHAR(MAX) = '';

    -- Get procName and numberOfParams from identifiers table
    SELECT @procName = [SPName], @numberOfParams = [AcceptedNumofParams]
    FROM  [ServiceWH].[MNGSPGatewayIdentifiers]
    WHERE [ActionId] = @ActionId;

    IF @procName IS NULL
    BEGIN
        RAISERROR('Please pass in a valid ActionId', 16, 1);
        RETURN;
    END
	 


    -- Build the parameter values string
    DECLARE @i INT = 1;
    WHILE @i <= @numberOfParams
    BEGIN
        DECLARE @paramValue NVARCHAR(MAX);
        
        -- Use CASE statement to select the correct parameter value
        SET @paramValue = 
            CASE @i
                WHEN 1 THEN @Param1
                WHEN 2 THEN @Param2
                WHEN 3 THEN @Param3
                WHEN 4 THEN @Param4
                WHEN 5 THEN @Param5
                WHEN 6 THEN @Param6
                WHEN 7 THEN @Param7
                WHEN 8 THEN @Param8
                WHEN 9 THEN @Param9
                WHEN 10 THEN @Param10
                WHEN 11 THEN @Param11
                WHEN 12 THEN @Param12
                WHEN 13 THEN @Param13
                WHEN 14 THEN @Param14
                WHEN 15 THEN @Param15
				 WHEN 16 THEN @Param16
            END;

        IF @paramValue IS NOT NULL
        BEGIN
            IF LEN(@paramValues) > 0
                SET @paramValues = @paramValues + ', ';
            
            -- Wrap the parameter value in quotes and escape any existing single quotes
            SET @paramValues = @paramValues + '''' + REPLACE(@paramValue, '''', '''''') + '''';
        END
        ELSE
        BEGIN
            IF LEN(@paramValues) > 0
                SET @paramValues = @paramValues + ', ';
            SET @paramValues = @paramValues + 'NULL';
        END

        SET @i = @i + 1;
    END





    -- Build the dynamic SQL
    SET @sql = 'EXEC ' + @procName + ' ' + @paramValues;

	PRINT(@sql)

    -- Execute the dynamic SQL
    EXEC sp_executesql @sql;


	    SET @RowCount = @@ROWCOUNT;
        SET @EndTime = GETUTCDATE();


 
    -- Queue the logging info after returning results
    -- This will run very quickly and not significantly delay the procedure's completion
    INSERT INTO  [ServiceWH].[MNGSPGatewayRunLog]
	(
	 [RunId]
	,[Caller]   
	,[ActionId]
	,[Timestamp] 
	,[Status]
	,ReturnedRows
	,[RunDurationMilliSeconds]
	 
) 
    VALUES (@RunID, @CallerId, @ActionId, @EndTime ,'Success', @RowCount, 
           DATEDIFF(MILLISECOND, @StartTime, @EndTime) )





	END TRY



	  BEGIN CATCH

        SET @Status = 'Failure';
        SET @RowCount = 0;
        SET @EndTime = GETUTCDATE();
        
      -- Capture the error details
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
    DECLARE @ErrorState INT = ERROR_STATE();


    -- Queue the logging info after returning results
    -- This will run very quickly and not significantly delay the procedure's completion
    INSERT INTO  [ServiceWH].[MNGSPGatewayRunLog]
	(
	 [RunId]
	,[Caller]   
	,[ActionId]
	,[Timestamp] 
	,[Status]
	,ReturnedRows
	,[RunDurationMilliSeconds]
	,ErrMessage
	 
) 
    VALUES (@RunID, @CallerId, @ActionId, @EndTime, @Status, @RowCount, 
           DATEDIFF(MILLISECOND, @StartTime, @EndTime) ,@ErrorMessage )


    -- Re-throw the error using RAISERROR  --  for the service to catch the error
    RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
    

			    END CATCH

END;



GO


