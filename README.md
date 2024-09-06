# SqlLearn

DROP PROCEDURE [MainWH].[sp_DWH_queryfactinstrumentstatistics];

exec [SDPWH].[MainWH].[sp_DWH_queryfactinstrumentstatistics]
@InstrumentIdList = '018ba6e6-7f5f-42be-a42a-6e15e28614f2,66a9ccbd-59d4-4c53-9020-27b354847911', @YearsList = '2024', @MonthsList = '2024-7'
;


exec [SDPWH].[MainWH].[sp_DWH_queryfactinstrumentstatistics]
@InstrumentIdList = '018ba6e6-7f5f-42be-a42a-6e15e28614f2,66a9ccbd-59d4-4c53-9020-27b354847911', @YearsList = '', @MonthsList = ''
;

exec [SDPWH].[MainWH].[sp_DWH_queryfactinstrumentstatistics]
@InstrumentIdList = '', @YearsList = '2023', @MonthsList = ''
;


exec [SDPWH].[MainWH].[sp_DWH_queryfactinstrumentstatistics]
@InstrumentIdList = '', @YearsList = '', @MonthsList = '2024-7'
;


exec [SDPWH].[MainWH].[sp_DWH_queryfactinstrumentstatistics]
@InstrumentIdList = '', @YearsList = '', @MonthsList = ''
;

exec [SDPWH].[MainWH].[sp_DWH_queryfactreratereccomendations]
@SourceEnv = ''
;

exec [SDPWH].[MainWH].[sp_DWH_queryfactreratereccomendations]
;



EXEC [SalesWH].[sp_DWH_queryfactinstrumentstatistics] 
'
BBG0107G4377,
BBG00L7LX529,
BBG000GKPSZ0
',
'
2023
',
'
2023-1,
2023-12
'
