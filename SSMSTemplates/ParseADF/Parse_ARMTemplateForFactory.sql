USE [Velera] ;
GO
IF SCHEMA_ID('ADF') IS NULL
    EXEC( 'CREATE SCHEMA ADF' ) ;
GO
IF SCHEMA_ID('ADF_HISTORY') IS NULL
    EXEC( 'CREATE SCHEMA ADF_HISTORY' ) ;
GO
RETURN ;

EXEC [ADF].[ImportARMTemplateForFactory] @Description = 'Dev', @ARMTemplateForFactory = '' ;
GO
CREATE OR ALTER PROCEDURE [ADF].[ImportARMTemplateForFactory]
    @Description           VARCHAR(4000)
  , @ARMTemplateForFactory NVARCHAR(MAX)
  , @UseTransaction        BIT = 1
AS
SET NOCOUNT ON ;

DECLARE
    @GETDATE DATETIME = GETDATE()
  , @SQL     NVARCHAR(MAX)
  , @cols    NVARCHAR(MAX) ;

SELECT
    @ARMTemplateForFactory = TRIM(CHAR(32) + CHAR(9) + CHAR(10) + CHAR(12) + CHAR(13)FROM @ARMTemplateForFactory)
  , @UseTransaction = ISNULL(@UseTransaction, 1) ;

IF OBJECT_ID('ADF.ARMTemplateForFactory') IS NULL
    CREATE TABLE [ADF].[ARMTemplateForFactory]
    (
        [ARMTemplateForFactoryId] INT            IDENTITY(1, 1) NOT NULL
      , [Description]             VARCHAR(4000)  NOT NULL
      , [ImportSuccess]           BIT            CONSTRAINT [ARMTemplateForFactory$ImportSuccess_Zero_DF] DEFAULT(( 0 )) NOT NULL
      , [DurationSec]             AS ( CONVERT(NUMERIC(20, 3), DATEDIFF(MILLISECOND, [CreatedDateTime], [UpdatedDateTime]) / ( 1000.0 )))
      , [JSONLength]              INT            NOT NULL
      , [JSONCompressedLength]    AS ( LEN([JSONCompressed]))
      , [JSON]                    AS ( CONVERT(NVARCHAR(MAX), DECOMPRESS([JSONCompressed])))
      , [JSONCompressed]          VARBINARY(MAX) NOT NULL
      , [ErrorMessage]            NVARCHAR(4000) NULL
      , [CreatedDateTime]         DATETIME2(3)   CONSTRAINT [ARMTemplateForFactory$CreatedDateTime_GETDATE_DF] DEFAULT( GETDATE()) NOT NULL
      , [UpdatedDateTime]         DATETIME2(3)   NULL
      , CONSTRAINT [ARMTemplateForFactory_PKC] PRIMARY KEY CLUSTERED( [ARMTemplateForFactoryId] ASC )
    ) ;

IF ISNULL(ISJSON(@ARMTemplateForFactory), 0) <> 1
    THROW 50000, 'Invalid @ARMTemplateForFactory, it is not a valid JSON!', 1 ;

INSERT [ADF].[ARMTemplateForFactory]( [Description], [JSONLength], [JSONCompressed] )
VALUES( @Description, LEN(@ARMTemplateForFactory), COMPRESS(@ARMTemplateForFactory)) ;

DECLARE @ARMTemplateForFactoryId INT = SCOPE_IDENTITY() ;

DROP TABLE IF EXISTS [#temp_yyy] ;
DROP TABLE IF EXISTS [#params_XXX] ;
DROP TABLE IF EXISTS [#temp_xxx] ;
DROP TABLE IF EXISTS [#recursive_resources] ;
DROP TABLE IF EXISTS [#pipelines] ;

SELECT
    ISNULL(CAST(REPLACE(REPLACE(JSON_VALUE([b].[value], '$.name'), '[concat(parameters(''factoryName''), ''/', ''), ''')]', '') AS VARCHAR(128)), '') AS [Pipeline]
  , JSON_QUERY([b].[value], '$.dependsOn') AS [dependsOn]
  , CAST([c].[key] AS VARCHAR(128)) COLLATE SQL_Latin1_General_CP1_CI_AS AS [ParameterName]
  , CAST(JSON_VALUE([c].[value], '$.type') AS VARCHAR(128)) AS [ParameterType]
  , JSON_VALUE([c].[value], '$.defaultValue') AS [ParameterDefaultValue]
INTO [#temp_yyy]
FROM OPENJSON(@ARMTemplateForFactory, '$.resources') AS [b]
OUTER APPLY OPENJSON([b].[value], '$.properties.parameters') AS [c]
WHERE JSON_VALUE([b].[value], '$.type') = 'Microsoft.DataFactory/factories/pipelines' ;

SELECT
    ISNULL(CAST([C].[key] AS VARCHAR(128)) COLLATE SQL_Latin1_General_CP1_CI_AS, '') AS [ParameterName]
  , CAST([d].[TYPE] AS VARCHAR(128)) AS [ParameterType]
  , CAST([d].[metadata] AS VARCHAR(MAX)) AS [ParameterMetadata]
  , CAST([d].[defaultValue] AS VARCHAR(MAX)) AS [ParameterDefaultValue]
INTO [#params_XXX]
FROM OPENJSON(@ARMTemplateForFactory) AS [B]
CROSS APPLY OPENJSON([B].[value]) AS [C]
OUTER APPLY OPENJSON([C].[value])
            WITH( [TYPE] VARCHAR(4000), [metadata] VARCHAR(4000), [defaultValue] VARCHAR(4000)) AS [d]
WHERE [B].[key] COLLATE SQL_Latin1_General_CP1_CI_AS = 'parameters' ;

SELECT
    ISNULL(CAST(REPLACE(REPLACE(JSON_VALUE([b].[value], '$.name'), '[concat(parameters(''factoryName''), ''/', ''), ''')]', '') AS VARCHAR(128)), '') AS [TriggerName]
  , CAST(JSON_VALUE([b].[value], '$.properties.runtimeState') AS VARCHAR(128)) AS [RunTimeState]
  , CAST([c].[KEY] AS INT) AS [PipeLineId]
  , CAST(JSON_VALUE([c].[value], '$.pipelineReference.referenceName') AS VARCHAR(128)) AS [PipeLine]
  , CAST([d].[Key] AS VARCHAR(128)) COLLATE SQL_Latin1_General_CP1_CI_AS AS [ParameterName]
  , [p].[ParameterType]
  , [p].[ParameterDefaultValue]
  , CAST(JSON_VALUE([b].[value], '$.properties.type') AS VARCHAR(128)) AS [TriggerType]
  , CAST(JSON_VALUE([b].[value], '$.properties.typeProperties.recurrence.frequency') AS VARCHAR(128)) AS [Frequency]
  , JSON_VALUE([b].[value], '$.properties.typeProperties.recurrence.interval') AS [Interval]
  , TRY_CAST(JSON_VALUE([b].[value], '$.properties.typeProperties.recurrence.startTime') AS DATETIME2(3)) AS [StartTime]
  , CAST(JSON_VALUE([b].[value], '$.properties.typeProperties.recurrence.timeZone') AS VARCHAR(128)) AS [TimeZone]
  , JSON_QUERY([b].[value], '$.properties.typeProperties.recurrence.schedule.minutes') AS [ScheduleMinutes]
  , JSON_QUERY([b].[value], '$.properties.typeProperties.recurrence.schedule.hours') AS [ScheduleHours]
INTO [#temp_xxx]
FROM OPENJSON(@ARMTemplateForFactory, '$.resources') AS [b]
OUTER APPLY OPENJSON([b].[value], '$.properties.pipelines') AS [c]
OUTER APPLY OPENJSON([c].[value], '$.parameters') AS [d]
LEFT JOIN [#params_XXX] AS [p] ON [p].[ParameterName] = REPLACE(REPLACE([d].[Value], '[parameters(''', ''), ''')]', '')
WHERE JSON_VALUE([b].[value], '$.type') = 'Microsoft.DataFactory/factories/triggers' ;

CREATE UNIQUE CLUSTERED INDEX [TRG] ON [#temp_xxx]( [TriggerName], [PipeLine], [ParameterName] ) ;

DROP TABLE IF EXISTS [#Parent] ;

SELECT
    ISNULL(CAST(REPLACE(REPLACE(JSON_VALUE([b].[value], '$.name'), '[concat(parameters(''factoryName''), ''/', ''), ''')]', '') AS VARCHAR(128)), '') AS [Pipeline]
  , ISNULL(ROW_NUMBER() OVER ( PARTITION BY JSON_VALUE([b].[value], '$.name')ORDER BY CAST([c].[key] AS INT)), 0) AS [L1_Id]
  , ISNULL([d].[name], '') AS [L1_Activity]
  , [d].[type] AS [L1_ActivityType]
  , [d].[dependsOn] AS [L1_DependsOn]
  , [d].[typeProperties] AS [L1_TypeProperties]
  , CAST(JSON_VALUE([d].[typeProperties], '$.items.value') AS VARCHAR(128)) AS [ForEachItems]
  , CAST(JSON_VALUE([d].[typeProperties], '$.items.type') AS VARCHAR(128)) AS [ForEachType]
  , CAST(JSON_VALUE([d].[typeProperties], '$.batchCount') AS INT) AS [ForEachBatchCount]
  , [e].[timeout] AS [TimeOut]
  , [e].[retry] AS [Retry]
  , [e].[retryIntervalInSeconds] AS [RetryIntervalInSeconds]
  , [e].[secureOutput] AS [SecureOutput]
  , [e].[secureInput] AS [SecureInput]
  , [f].[dataIntegrationUnits] AS [DataIntegrationUnits]
  , [f].[enableStaging] AS [EnableStaging]
INTO [#Parent]
FROM OPENJSON(@ARMTemplateForFactory, '$.resources') AS [b]
OUTER APPLY OPENJSON([b].[value], '$.properties.activities') AS [c]
OUTER APPLY OPENJSON([c].[value])
            WITH( [name] VARCHAR(128), [type] VARCHAR(128), [dependsOn] NVARCHAR(MAX) AS JSON, [typeProperties] NVARCHAR(MAX) AS JSON, [policy] NVARCHAR(MAX) AS JSON ) AS [d]
OUTER APPLY OPENJSON([d].[policy])
            WITH( [timeout] VARCHAR(256), [retry] INT, [retryIntervalInSeconds] INT, [secureOutput] BIT, [secureInput] BIT ) AS [e]
OUTER APPLY OPENJSON([d].[typeProperties])
            WITH( [dataIntegrationUnits] INT, [enableStaging] BIT ) AS [f]
WHERE JSON_VALUE([b].[value], '$.type') = 'Microsoft.DataFactory/factories/pipelines' ;

CREATE UNIQUE CLUSTERED INDEX [xxsx] ON [#Parent]( [Pipeline], [L1_Id] ) ;

DROP TABLE IF EXISTS [#L2] ;

SELECT
    [k].[Pipeline]
  , [k].[L1_Id]
  , [k].[L1_Activity]
  , [k].[L1_ActivityType]
  , ROW_NUMBER() OVER ( PARTITION BY [k].[Pipeline], [k].[L1_Id] ORDER BY [k].[L2_IFCondition], [k].[L2_Id] ) AS [L2_Id]
  , [k].[L2_Activity]
  , [k].[L2_ActivityType]
  , NULL AS [L2_SwitchValue]
  , NULL AS [L1_SwitchExpressionValue]
  , NULL AS [L1_SwitchExpressionType]
  , [k].[L1_IFExpressionValue]
  , [k].[L1_IFExpressionType]
  , [k].[L2_IFCondition]
  , [k].[L2_TypeProperties]
  , [k].[TimeOut] AS [TimeOut]
  , [k].[Retry] AS [Retry]
  , [k].[RetryIntervalInSeconds] AS [RetryIntervalInSeconds]
  , [k].[SecureOutput] AS [SecureOutput]
  , [k].[SecureInput] AS [SecureInput]
  , [k].[DataIntegrationUnits]
  , [k].[EnableStaging]
INTO [#L2]
FROM( SELECT
          [p].[Pipeline]
        , [p].[L1_Id]
        , [p].[L1_Activity]
        , [p].[L1_ActivityType]
        , [f].[Id] AS [L2_Id]
        , [f].[name] AS [L2_Activity]
        , [f].[type] AS [L2_ActivityType]
        , JSON_VALUE([L1_TypeProperties], '$.expression.value') AS [L1_IFExpressionValue]
        , JSON_VALUE([L1_TypeProperties], '$.expression.type') AS [L1_IFExpressionType]
        , 'False' AS [L2_IFCondition]
        , [f].[typeProperties] AS [L2_TypeProperties]
        , [g].[timeout] AS [TimeOut]
        , [g].[retry] AS [Retry]
        , [g].[retryIntervalInSeconds] AS [RetryIntervalInSeconds]
        , [g].[secureOutput] AS [SecureOutput]
        , [g].[secureInput] AS [SecureInput]
        , [h].[dataIntegrationUnits] AS [DataIntegrationUnits]
        , [h].[enableStaging] AS [EnableStaging]
      FROM [#Parent] AS [p]
      CROSS APPLY( SELECT
                       ROW_NUMBER() OVER ( ORDER BY( SELECT 0 )) AS [Id]
                     , *
                   FROM OPENJSON([p].[L1_TypeProperties], '$.ifFalseActivities')
                        WITH( [name] VARCHAR(128), [type] VARCHAR(128), [typeProperties] NVARCHAR(MAX) AS JSON, [policy] NVARCHAR(MAX) AS JSON ) AS [f] ) AS [f]
      OUTER APPLY OPENJSON([f].[policy])
                  WITH( [timeout] VARCHAR(256), [retry] INT, [retryIntervalInSeconds] INT, [secureOutput] BIT, [secureInput] BIT ) AS [g]
      OUTER APPLY OPENJSON([f].[typeProperties])
                  WITH( [dataIntegrationUnits] INT, [enableStaging] BIT ) AS [h]
      WHERE [p].[L1_ActivityType] = 'IFCondition'
      UNION ALL
      SELECT
          [p].[Pipeline]
        , [p].[L1_Id]
        , [p].[L1_Activity]
        , [p].[L1_ActivityType]
        , [f].[Id] AS [L2_Id]
        , [f].[name] AS [L2_Activity]
        , [f].[type] AS [L2_ActivityType]
        , JSON_VALUE([L1_TypeProperties], '$.expression.value') AS [L1_IFExpressionValue]
        , JSON_VALUE([L1_TypeProperties], '$.expression.type') AS [L1_IFExpressionType]
        , 'True' AS [L2_IFCondition]
        , [f].[typeProperties] AS [L2_TypeProperties]
        , [g].[timeout] AS [TimeOut]
        , [g].[retry] AS [Retry]
        , [g].[retryIntervalInSeconds] AS [RetryIntervalInSeconds]
        , [g].[secureOutput] AS [SecureOutput]
        , [g].[secureInput] AS [SecureInput]
        , [h].[dataIntegrationUnits] AS [DataIntegrationUnits]
        , [h].[enableStaging] AS [EnableStaging]
      FROM [#Parent] AS [p]
      CROSS APPLY( SELECT
                       ROW_NUMBER() OVER ( ORDER BY( SELECT 0 )) AS [Id]
                     , *
                   FROM OPENJSON([p].[L1_TypeProperties], '$.ifTrueActivities')
                        WITH( [name] VARCHAR(128), [type] VARCHAR(128), [typeProperties] NVARCHAR(MAX) AS JSON, [policy] NVARCHAR(MAX) AS JSON ) AS [f] ) AS [f]
      OUTER APPLY OPENJSON([f].[policy])
                  WITH( [timeout] VARCHAR(256), [retry] INT, [retryIntervalInSeconds] INT, [secureOutput] BIT, [secureInput] BIT ) AS [g]
      OUTER APPLY OPENJSON([f].[typeProperties])
                  WITH( [dataIntegrationUnits] INT, [enableStaging] BIT ) AS [h]
      WHERE [p].[L1_ActivityType] = 'IFCondition' ) AS [k]
UNION ALL
SELECT
    [p].[Pipeline]
  , [p].[L1_Id]
  , [p].[L1_Activity]
  , [p].[L1_ActivityType]
  , ROW_NUMBER() OVER ( PARTITION BY [p].[Pipeline], [p].[L1_Id] ORDER BY JSON_VALUE([e].[value], '$.value'), [f].[Id] ) AS [L3_Id]
  , [f].[name] AS [L2_Activity]
  , [f].[type] AS [L2_ActivityType]
  , JSON_VALUE([e].[value], '$.value') AS [L2_SwitchValue]
  , JSON_VALUE([L1_TypeProperties], '$.on.value') AS [L1_SwitchExpressionValue]
  , JSON_VALUE([L1_TypeProperties], '$.on.type') AS [L1_SwitchExpressionType]
  , NULL AS [L1_IFExpressionValue]
  , NULL AS [L1_IFExpressionType]
  , NULL AS [L2_IFCondition]
  , [f].[typeProperties] AS [L2_TypeProperties]
  , [g].[timeout] AS [TimeOut]
  , [g].[retry] AS [Retry]
  , [g].[retryIntervalInSeconds] AS [RetryIntervalInSeconds]
  , [g].[secureOutput] AS [SecureOutput]
  , [g].[secureInput] AS [SecureInput]
  , [h].[dataIntegrationUnits] AS [DataIntegrationUnits]
  , [h].[enableStaging] AS [EnableStaging]
FROM [#Parent] AS [p]
CROSS APPLY OPENJSON([L1_TypeProperties], '$.cases') AS [e]
CROSS APPLY( SELECT
                 ROW_NUMBER() OVER ( ORDER BY( SELECT 0 )) AS [Id]
               , *
             FROM OPENJSON([e].[value], '$.activities')
                  WITH( [name] VARCHAR(128), [type] VARCHAR(128), [typeProperties] NVARCHAR(MAX) AS JSON, [policy] NVARCHAR(MAX) AS JSON ) AS [f] ) AS [f]
OUTER APPLY OPENJSON([f].[policy])
            WITH( [timeout] VARCHAR(256), [retry] INT, [retryIntervalInSeconds] INT, [secureOutput] BIT, [secureInput] BIT ) AS [g]
OUTER APPLY OPENJSON([f].[typeProperties])
            WITH( [dataIntegrationUnits] INT, [enableStaging] BIT ) AS [h]
WHERE [p].[L1_ActivityType] = 'switch'
UNION ALL
SELECT
    [p].[Pipeline]
  , [p].[L1_Id]
  , [p].[L1_Activity]
  , [p].[L1_ActivityType]
  , [f].[Id] AS [L2_Id]
  , [f].[name] AS [L2_Activity]
  , [f].[type] AS [L2_ActivityType]
  , NULL AS [L2_SwitchValue]
  , NULL AS [L1_SwitchExpressionValue]
  , NULL AS [L1_SwitchExpressionType]
  , NULL AS [L1_IFExpressionValue]
  , NULL AS [L1_IFExpressionType]
  , NULL AS [L2_IFCondition]
  , [f].[typeProperties] AS [L2_TypeProperties]
  , [g].[timeout] AS [TimeOut]
  , [g].[retry] AS [Retry]
  , [g].[retryIntervalInSeconds] AS [RetryIntervalInSeconds]
  , [g].[secureOutput] AS [SecureOutput]
  , [g].[secureInput] AS [SecureInput]
  , [h].[dataIntegrationUnits] AS [DataIntegrationUnits]
  , [h].[enableStaging] AS [EnableStaging]
FROM [#Parent] AS [p]
CROSS APPLY( SELECT
                 ROW_NUMBER() OVER ( ORDER BY( SELECT 0 )) AS [Id]
               , *
             FROM OPENJSON([p].[L1_TypeProperties], '$.activities')
                  WITH( [name] VARCHAR(128), [type] VARCHAR(128), [typeProperties] NVARCHAR(MAX) AS JSON, [policy] NVARCHAR(MAX) AS JSON ) AS [f] ) AS [f]
OUTER APPLY OPENJSON([f].[policy])
            WITH( [timeout] VARCHAR(256), [retry] INT, [retryIntervalInSeconds] INT, [secureOutput] BIT, [secureInput] BIT ) AS [g]
OUTER APPLY OPENJSON([f].[typeProperties])
            WITH( [dataIntegrationUnits] INT, [enableStaging] BIT ) AS [h]
WHERE [p].[L1_ActivityType] = 'ForEach' ;

CREATE UNIQUE CLUSTERED INDEX [L2] ON [#L2]( [Pipeline], [L1_Id], [L2_Id] ) ;

DROP TABLE IF EXISTS [#L3] ;

SELECT
    [k].[Pipeline]
  , [k].[L1_Id]
  , [k].[L1_Activity]
  , [k].[L1_ActivityType]
  , [k].[L2_Id]
  , [k].[L2_Activity]
  , [k].[L2_ActivityType]
  , ROW_NUMBER() OVER ( PARTITION BY [k].[Pipeline], [k].[L1_Id], [k].[L2_Id] ORDER BY [k].[L3_IFCondition], [k].[L3_Id] ) AS [L3_Id]
  , [k].[L3_Activity]
  , [k].[L3_ActivityType]
  , NULL AS [L3_SwitchValue]
  , NULL AS [L2_SwitchExpressionValue]
  , NULL AS [L2_SwitchExpressionType]
  , [k].[L2_IFExpressionValue]
  , [k].[L2_IFExpressionType]
  , [k].[L3_IFCondition]
  , [k].[L3_TypeProperties]
  , [k].[TimeOut] AS [TimeOut]
  , [k].[Retry] AS [Retry]
  , [k].[RetryIntervalInSeconds] AS [RetryIntervalInSeconds]
  , [k].[SecureOutput] AS [SecureOutput]
  , [k].[SecureInput] AS [SecureInput]
  , [k].[DataIntegrationUnits]
  , [k].[EnableStaging]
INTO [#L3]
FROM( SELECT
          [p].[Pipeline]
        , [p].[L1_Id]
        , [p].[L1_Activity]
        , [p].[L1_ActivityType]
        , [p].[L2_Id]
        , [p].[L2_Activity]
        , [p].[L2_ActivityType]
        , [f].[Id] AS [L3_Id]
        , [f].[name] AS [L3_Activity]
        , [f].[type] AS [L3_ActivityType]
        , JSON_VALUE([p].[L2_TypeProperties], '$.expression.value') AS [L2_IFExpressionValue]
        , JSON_VALUE([p].[L2_TypeProperties], '$.expression.type') AS [L2_IFExpressionType]
        , 'False' AS [L3_IFCondition]
        , [f].[typeProperties] AS [L3_TypeProperties]
        , [g].[timeout] AS [TimeOut]
        , [g].[retry] AS [Retry]
        , [g].[retryIntervalInSeconds] AS [RetryIntervalInSeconds]
        , [g].[secureOutput] AS [SecureOutput]
        , [g].[secureInput] AS [SecureInput]
        , [h].[dataIntegrationUnits] AS [DataIntegrationUnits]
        , [h].[enableStaging] AS [EnableStaging]
      FROM [#L2] AS [p]
      CROSS APPLY( SELECT
                       ROW_NUMBER() OVER ( ORDER BY( SELECT 0 )) AS [Id]
                     , *
                   FROM OPENJSON([p].[L2_TypeProperties], '$.ifFalseActivities')
                        WITH( [name] VARCHAR(128), [type] VARCHAR(128), [typeProperties] NVARCHAR(MAX) AS JSON, [policy] NVARCHAR(MAX) AS JSON ) AS [f] ) AS [f]
      OUTER APPLY OPENJSON([f].[policy])
                  WITH( [timeout] VARCHAR(256), [retry] INT, [retryIntervalInSeconds] INT, [secureOutput] BIT, [secureInput] BIT ) AS [g]
      OUTER APPLY OPENJSON([f].[typeProperties])
                  WITH( [dataIntegrationUnits] INT, [enableStaging] BIT ) AS [h]
      WHERE [p].[L2_ActivityType] = 'IFCondition'
      UNION ALL
      SELECT
          [p].[Pipeline]
        , [p].[L1_Id]
        , [p].[L1_Activity]
        , [p].[L1_ActivityType]
        , [p].[L2_Id]
        , [p].[L2_Activity]
        , [p].[L2_ActivityType]
        , [f].[Id] AS [L3_Id]
        , [f].[name] AS [L3_Activity]
        , [f].[type] AS [L3_ActivityType]
        , JSON_VALUE([p].[L2_TypeProperties], '$.expression.value') AS [L2_IFExpressionValue]
        , JSON_VALUE([p].[L2_TypeProperties], '$.expression.type') AS [L2_IFExpressionType]
        , 'True' AS [L3_IFCondition]
        , [f].[typeProperties] AS [L3_TypeProperties]
        , [g].[timeout] AS [TimeOut]
        , [g].[retry] AS [Retry]
        , [g].[retryIntervalInSeconds] AS [RetryIntervalInSeconds]
        , [g].[secureOutput] AS [SecureOutput]
        , [g].[secureInput] AS [SecureInput]
        , [h].[dataIntegrationUnits] AS [DataIntegrationUnits]
        , [h].[enableStaging] AS [EnableStaging]
      FROM [#L2] AS [p]
      CROSS APPLY( SELECT
                       ROW_NUMBER() OVER ( ORDER BY( SELECT 0 )) AS [Id]
                     , *
                   FROM OPENJSON([p].[L2_TypeProperties], '$.ifTrueActivities')
                        WITH( [name] VARCHAR(128), [type] VARCHAR(128), [typeProperties] NVARCHAR(MAX) AS JSON, [policy] NVARCHAR(MAX) AS JSON ) AS [f] ) AS [f]
      OUTER APPLY OPENJSON([f].[policy])
                  WITH( [timeout] VARCHAR(256), [retry] INT, [retryIntervalInSeconds] INT, [secureOutput] BIT, [secureInput] BIT ) AS [g]
      OUTER APPLY OPENJSON([f].[typeProperties])
                  WITH( [dataIntegrationUnits] INT, [enableStaging] BIT ) AS [h]
      WHERE [p].[L2_ActivityType] = 'IFCondition' ) AS [k]
UNION ALL
SELECT
    [p].[Pipeline]
  , [p].[L1_Id]
  , [p].[L1_Activity]
  , [p].[L1_ActivityType]
  , [p].[L2_Id]
  , [p].[L2_Activity]
  , [p].[L2_ActivityType]
  , ROW_NUMBER() OVER ( PARTITION BY [p].[Pipeline], [p].[L1_Id], [p].[L2_Id] ORDER BY JSON_VALUE([e].[value], '$.value'), [f].[Id] ) AS [L3_Id]
  , [f].[name] AS [L3_Activity]
  , [f].[type] AS [L3_ActivityType]
  , JSON_VALUE([e].[value], '$.value') AS [L3_SwitchValue]
  , JSON_VALUE([p].[L2_TypeProperties], '$.on.value') AS [L2_SwitchExpressionValue]
  , JSON_VALUE([p].[L2_TypeProperties], '$.on.type') AS [L2_SwitchExpressionType]
  , NULL AS [L2_IFExpressionValue]
  , NULL AS [L2_IFExpressionType]
  , NULL AS [L3_IFCondition]
  , [f].[typeProperties] AS [L3_TypeProperties]
  , [g].[timeout] AS [TimeOut]
  , [g].[retry] AS [Retry]
  , [g].[retryIntervalInSeconds] AS [RetryIntervalInSeconds]
  , [g].[secureOutput] AS [SecureOutput]
  , [g].[secureInput] AS [SecureInput]
  , [h].[dataIntegrationUnits] AS [DataIntegrationUnits]
  , [h].[enableStaging] AS [EnableStaging]
FROM [#L2] AS [p]
CROSS APPLY OPENJSON([p].[L2_TypeProperties], '$.cases') AS [e]
CROSS APPLY( SELECT
                 ROW_NUMBER() OVER ( ORDER BY( SELECT 0 )) AS [Id]
               , *
             FROM OPENJSON([e].[value], '$.activities')
                  WITH( [name] VARCHAR(128), [type] VARCHAR(128), [typeProperties] NVARCHAR(MAX) AS JSON, [policy] NVARCHAR(MAX) AS JSON ) AS [f] ) AS [f]
OUTER APPLY OPENJSON([f].[policy])
            WITH( [timeout] VARCHAR(256), [retry] INT, [retryIntervalInSeconds] INT, [secureOutput] BIT, [secureInput] BIT ) AS [g]
OUTER APPLY OPENJSON([f].[typeProperties])
            WITH( [dataIntegrationUnits] INT, [enableStaging] BIT ) AS [h]
WHERE [p].[L2_ActivityType] = 'switch'
UNION ALL
SELECT
    [p].[Pipeline]
  , [p].[L1_Id]
  , [p].[L1_Activity]
  , [p].[L1_ActivityType]
  , [p].[L2_Id]
  , [p].[L2_Activity]
  , [p].[L2_ActivityType]
  , [f].[Id] AS [L3_Id]
  , [f].[name] AS [L3_Activity]
  , [f].[type] AS [L3_ActivityType]
  , NULL AS [L3_SwitchValue]
  , NULL AS [L2_SwitchExpressionValue]
  , NULL AS [L2_SwitchExpressionType]
  , NULL AS [L2_IFExpressionValue]
  , NULL AS [L2_IFExpressionType]
  , NULL AS [L3_IFCondition]
  , [f].[typeProperties] AS [L3_TypeProperties]
  , [g].[timeout] AS [TimeOut]
  , [g].[retry] AS [Retry]
  , [g].[retryIntervalInSeconds] AS [RetryIntervalInSeconds]
  , [g].[secureOutput] AS [SecureOutput]
  , [g].[secureInput] AS [SecureInput]
  , [h].[dataIntegrationUnits] AS [DataIntegrationUnits]
  , [h].[enableStaging] AS [EnableStaging]
FROM [#L2] AS [p]
CROSS APPLY( SELECT
                 ROW_NUMBER() OVER ( ORDER BY( SELECT 0 )) AS [Id]
               , *
             FROM OPENJSON([p].[L2_TypeProperties], '$.activities')
                  WITH( [name] VARCHAR(128), [type] VARCHAR(128), [typeProperties] NVARCHAR(MAX) AS JSON, [policy] NVARCHAR(MAX) AS JSON ) AS [f] ) AS [f]
OUTER APPLY OPENJSON([f].[policy])
            WITH( [timeout] VARCHAR(256), [retry] INT, [retryIntervalInSeconds] INT, [secureOutput] BIT, [secureInput] BIT ) AS [g]
OUTER APPLY OPENJSON([f].[typeProperties])
            WITH( [dataIntegrationUnits] INT, [enableStaging] BIT ) AS [h]
WHERE [p].[L2_ActivityType] = 'ForEach' ;

CREATE UNIQUE CLUSTERED INDEX [L2] ON [#L3]( [Pipeline], [L1_Id], [L2_Id], [L3_Id] ) ;

    BEGIN TRY
        IF @UseTransaction = 1
            BEGIN TRANSACTION ;

        DROP TABLE IF EXISTS [ADF].[PipeLines] ;
        DROP TABLE IF EXISTS [ADF].[PipeLineActivitiesPivot] ;
        DROP TABLE IF EXISTS [ADF].[PipeLineActivities] ;
        DROP TABLE IF EXISTS [ADF].[PipeLineAllActivities] ;
        DROP TABLE IF EXISTS [ADF].[PipeLineDependencies] ;
        DROP TABLE IF EXISTS [ADF].[PipeLineDependenciesGrouped] ;
        DROP TABLE IF EXISTS [ADF].[PipeLineParameters] ;
        DROP TABLE IF EXISTS [ADF].[Triggers] ;
        DROP TABLE IF EXISTS [ADF].[TriggerTimes] ;
        DROP TABLE IF EXISTS [ADF].[TriggerParameters] ;
        DROP TABLE IF EXISTS [ADF].[TriggerPipeLines] ;
        DROP TABLE IF EXISTS [ADF].[LinkedServices] ;
        DROP TABLE IF EXISTS [ADF].[DataFlowDependencies] ;
        DROP TABLE IF EXISTS [ADF].[DataFlows] ;
        DROP TABLE IF EXISTS [ADF].[DataSets] ;
        DROP TABLE IF EXISTS [ADF].[IntegrationRunTimes] ;
        DROP TABLE IF EXISTS [ADF].[StoredProcedureOrLookup] ;
        DROP TABLE IF EXISTS [ADF].[StoredProcedureOrLookupParameters] ;
        DROP TABLE IF EXISTS [ADF].[SQLQueries] ;
        DROP TABLE IF EXISTS [ADF].[ActivityProperties] ;
        DROP TABLE IF EXISTS [ADF].[ActivityQueries] ;
        DROP TABLE IF EXISTS [ADF].[VariableAssigments] ;
        DROP TABLE IF EXISTS [ADF].[Parameters] ;
        DROP TABLE IF EXISTS [ADF].[GlobalParameters] ;
        DROP TABLE IF EXISTS [ADF].[ForEach] ;
        DROP TABLE IF EXISTS [ADF].[ForEach] ;
        DROP TABLE IF EXISTS [#param1] ;
        DROP TABLE IF EXISTS [#param2] ;
        DROP TABLE IF EXISTS [ADF].[ActivityReferences] ;
        DROP TABLE IF EXISTS [ADF].[DataFlowSteps] ;
        DROP TABLE IF EXISTS [ADF].[CopyActivities] ;
        DROP TABLE IF EXISTS [#ActivityAttributes] ;

        ;WITH [PipelineCopyActivities]
        AS ( SELECT
                 REPLACE(REPLACE(JSON_VALUE([P].[value], '$.name'), '[concat(parameters(''factoryName''), ''/', ''), ''')]', '') AS [PipelineName]
               , JSON_VALUE([A].[value], '$.name') AS [ActivityName]
               , [A].[value] AS [ActivityJson]
             FROM OPENJSON(@ARMTemplateForFactory, '$.resources') AS [P]
             CROSS APPLY OPENJSON([P].[value], '$.properties.activities') AS [A]
             WHERE JSON_VALUE([P].[value], '$.type') = 'Microsoft.DataFactory/factories/pipelines' AND JSON_VALUE([A].[value], '$.type') = 'Copy' )
            , [RecursiveAttributes]
        AS (
           -- Base Case: Top level properties
           SELECT
               [PipelineCopyActivities].[PipelineName]
             , [PipelineCopyActivities].[ActivityName]
             , CAST([key] AS NVARCHAR(400)) AS [PropertyPath]
             , [value] AS [Value]
             , [type] AS [Type]
           FROM [PipelineCopyActivities]
           CROSS APPLY OPENJSON([PipelineCopyActivities].[ActivityJson])
           UNION ALL

           -- Recursive Case: Drill down into Objects (Type 5), stop at Arrays (Type 4)
           SELECT
               [Parent].[PipelineName]
             , [Parent].[ActivityName]
             , CAST([Parent].[PropertyPath] + '.' + [Child].[key] AS NVARCHAR(400)) AS [PropertyPath]
             , [Child].[value] AS [Value]
             , [Child].[type] AS [Type]
           FROM [RecursiveAttributes] AS [Parent]
           CROSS APPLY OPENJSON([Parent].[Value]) AS [Child]
           WHERE [Parent].[Type] = 5 )
        SELECT
            [RecursiveAttributes].[PipelineName]
          , [RecursiveAttributes].[ActivityName]
          , [RecursiveAttributes].[PropertyPath] AS [Attribute]
          , [RecursiveAttributes].[Value]
        INTO [#ActivityAttributes]
        FROM [RecursiveAttributes]
        WHERE [RecursiveAttributes].[Type] != 5 -- Exclude container objects, keep only leaves (Scalars/Arrays)
        OPTION( MAXRECURSION 100 ) ;

        -------------------------------------------------------------------------------
        -- 2. Build Dynamic Column List
        -------------------------------------------------------------------------------
        -- This finds all unique attribute paths found across ALL activities (e.g., "typeProperties.source.queryTimeout")
        SELECT @cols = STRING_AGG(CAST(QUOTENAME([DistinctCols].[Attribute]) AS VARCHAR(MAX)), ',') WITHIN GROUP(ORDER BY [DistinctCols].[Attribute])
        FROM( SELECT DISTINCT [#ActivityAttributes].[Attribute] FROM [#ActivityAttributes] ) AS [DistinctCols] ;

        -------------------------------------------------------------------------------
        -- 3. Construct and Execute the Pivot Query
        -------------------------------------------------------------------------------
        SET @SQL = N'
    SELECT
        ISNULL(CAST(PipelineName COLLATE SQL_Latin1_General_CP1_CI_AS AS VARCHAR(128)), '''') AS PipelineName,
        ISNULL(CAST(ActivityName COLLATE SQL_Latin1_General_CP1_CI_AS AS VARCHAR(128)), '''') AS ActivityName,
        ' + @cols + N'
    INTO [ADF].[CopyActivities]
    FROM
        (SELECT PipelineName, ActivityName, Attribute, NULLIF(Value, ''[]'') AS Value FROM #ActivityAttributes) AS SourceTable
    PIVOT
        (
            MAX(Value)
            FOR Attribute IN (' + @cols + N')
        ) AS PivotTable
    ORDER BY PipelineName, ActivityName;
'       ;

        -- Execute the dynamic SQL
        EXEC [sys].[sp_executesql] @SQL ;

        -- Cleanup
        DROP TABLE [#ActivityAttributes] ;

        ALTER TABLE [ADF].[CopyActivities] ADD CONSTRAINT [CopyActivities_PKC] PRIMARY KEY CLUSTERED( [PipelineName], [ActivityName] ) ;

        WITH [Resources]
        AS ( SELECT
                 JSON_VALUE([value], '$.type') AS [ResourceType]
               -- Capture the raw ARM name (e.g., "[concat(parameters('factoryName'), '/MyDataset')]")
               , JSON_VALUE([value], '$.name') AS [RawName]
               -- Capture the properties object for deeper parsing
               , JSON_QUERY([value], '$.properties') AS [Properties]
             FROM OPENJSON(@ARMTemplateForFactory, '$.resources'))
           /*
              CTE 2: Parse Dataflows.
              We drill into 'sources', 'sinks', and 'transformations' arrays inside the properties.
              OPENJSON on an array returns the 'key' (index) which serves as the Step Order.
           */
           , [DataFlowSteps]
        AS ( SELECT
                 -- Clean the Dataflow Name (remove ARM wrapper)
                 REPLACE(REPLACE([R].[RawName], '[concat(parameters(''factoryName''), ''/', ''), ''')]', '') AS [DataflowName]
               , [Steps].[StepType]
               , CAST([Steps].[Key] AS INT) AS [StepOrder]
               , JSON_VALUE([Steps].[Value], '$.name') AS [StepName]
               -- Get the referenced dataset name (this is usually clean, e.g., "Dim_Customer")
               , JSON_VALUE([Steps].[Value], '$.dataset.referenceName') AS [DatasetRef]
             FROM [Resources] AS [R]
             CROSS APPLY(
                        -- Combine Sources, Sinks, and Transformations into a unified list
                        SELECT
                            'Source' AS [StepType]
                          , [key]
                          , [value]
                        FROM OPENJSON([R].[Properties], '$.typeProperties.sources')
                        UNION ALL
                        SELECT
                            'Sink' AS [StepType]
                          , [key]
                          , [value]
                        FROM OPENJSON([R].[Properties], '$.typeProperties.sinks')
                        UNION ALL
                        SELECT
                            'Transformation' AS [StepType]
                          , [key]
                          , [value]
                        FROM OPENJSON([R].[Properties], '$.typeProperties.transformations')
                        -- Only include transformations that actually reference a dataset
                        WHERE JSON_VALUE([value], '$.dataset.referenceName') IS NOT NULL ) AS [Steps]
             WHERE [R].[ResourceType] = 'Microsoft.DataFactory/factories/dataflows' )
           /*
              CTE 3: Parse Datasets.
              We extract specific attributes like Folder, LinkedService, and Schema/Table/File paths.
           */
           , [DatasetDefinitions]
        AS ( SELECT
                 -- Clean the Dataset Name to match the reference in Dataflows
                 REPLACE(REPLACE([R].[RawName], '[concat(parameters(''factoryName''), ''/', ''), ''')]', '') AS [DatasetName]
               , JSON_VALUE([R].[Properties], '$.type') AS [DatasetType]
               , JSON_VALUE([R].[Properties], '$.linkedServiceName.referenceName') AS [LinkedServiceName]
               , JSON_VALUE([R].[Properties], '$.folder.name') AS [FolderName]
               -- Attributes for Database Tables
               , JSON_VALUE([R].[Properties], '$.typeProperties.schema') AS [DbSchema]
               , JSON_VALUE([R].[Properties], '$.typeProperties.table') AS [DbTable]
               -- Attributes for Files/Storage
               , JSON_VALUE([R].[Properties], '$.typeProperties.location.fileSystem') AS [FileSystem]
               , JSON_VALUE([R].[Properties], '$.typeProperties.location.folderPath') AS [FolderPath]
               , JSON_VALUE([R].[Properties], '$.typeProperties.location.fileName') AS [FileName]
             FROM [Resources] AS [R]
             WHERE [R].[ResourceType] = 'Microsoft.DataFactory/factories/datasets' )

        /*
           Final Query: Join Dataflow Steps with Dataset Definitions
        */
        SELECT
            ISNULL(CAST([DF].[DataflowName] AS VARCHAR(128)) COLLATE SQL_Latin1_General_CP1_CI_AS, '') AS [DataflowName]
          , ISNULL(CAST([DF].[StepType] AS VARCHAR(128)) COLLATE SQL_Latin1_General_CP1_CI_AS, '') AS [StepType]
          , ISNULL([DF].[StepOrder], 0) AS [StepOrder]
          , ISNULL(CAST([DF].[StepName] AS VARCHAR(128)) COLLATE SQL_Latin1_General_CP1_CI_AS, '') AS [StepName]
          , ISNULL(CAST([DF].[DatasetRef] AS VARCHAR(128)) COLLATE SQL_Latin1_General_CP1_CI_AS, '') AS [DatasetReference]
          , ISNULL(CAST([DS].[DatasetType] AS VARCHAR(128)) COLLATE SQL_Latin1_General_CP1_CI_AS, '') AS [DatasetType]
          , ISNULL(CAST([DS].[LinkedServiceName] AS VARCHAR(128)) COLLATE SQL_Latin1_General_CP1_CI_AS, '') AS [LinkedServiceName]
          , ISNULL(CAST([DS].[FolderName] AS VARCHAR(128)) COLLATE SQL_Latin1_General_CP1_CI_AS, '') AS [FolderName]

          -- Logic to display a readable "Target" location regardless of type
          , ISNULL(CAST(COALESCE([DS].[DbSchema] + '.' + [DS].[DbTable] -- SQL Table (Schema.Table)
                               , [DS].[FileSystem] + '/' + [DS].[FolderPath] + '/' + [DS].[FileName] -- Blob/ADLS Full Path
                               , [DS].[FolderPath] + '/' + [DS].[FileName] -- Partial Path
                               , [DS].[DbTable] -- Table only
                               , 'Dynamic/Parameter') -- Fallback
                AS VARCHAR(8000)), '')COLLATE SQL_Latin1_General_CP1_CI_AS AS [TargetLocation]
        INTO [ADF].[DataFlowSteps]
        FROM [DataFlowSteps] AS [DF]
        LEFT JOIN [DatasetDefinitions] AS [DS] ON [DF].[DatasetRef] = [DS].[DatasetName]
        ORDER BY [DF].[DataflowName]
               , CASE [DF].[StepType] WHEN 'Source' THEN 1 WHEN 'Transformation' THEN 2 WHEN 'Sink' THEN 3 END -- Logical Sort
               , [DF].[StepOrder] ;

        ALTER TABLE [ADF].[DataFlowSteps] ADD CONSTRAINT [DataFlowSteps_PKC] PRIMARY KEY CLUSTERED( [DataflowName], [StepType], [StepOrder] ) ;

        SELECT
            [p].[Pipeline]
          , [p].[L1_Id] AS [Id]
          , [p].[L1_Activity] AS [Activity]
          , [p].[L1_ActivityType] AS [ActivityType]
          , [p].[ForEachItems]
          , [p].[ForEachType]
          , [p].[ForEachBatchCount]
        INTO [ADF].[ForEach]
        FROM [#Parent] AS [p]
        WHERE [p].[L1_ActivityType] = 'ForEach' ;

        ALTER TABLE [ADF].[ForEach] ADD CONSTRAINT [ForEach_PKC] PRIMARY KEY CLUSTERED( [Pipeline], [Activity] ) ;

        SELECT
            ISNULL(CAST([b].[key] AS VARCHAR(256)), '')COLLATE SQL_Latin1_General_CP1_CI_AS AS [ParameterName]
          , JSON_VALUE([b].[value], '$.type')COLLATE SQL_Latin1_General_CP1_CI_AS AS [DataType]
          , JSON_VALUE([b].[value], '$.metadata')COLLATE SQL_Latin1_General_CP1_CI_AS AS [Metadata]
          , JSON_VALUE([b].[value], '$.defaultValue')COLLATE SQL_Latin1_General_CP1_CI_AS AS [DefaultValue]
        INTO [#param1]
        FROM OPENJSON(@ARMTemplateForFactory, '$.parameters') AS [b] ;

        SELECT
            ISNULL(CAST([c].[key] AS VARCHAR(256)) COLLATE SQL_Latin1_General_CP1_CI_AS, '') AS [ParameterName]
          , JSON_VALUE([c].[value], '$.type')COLLATE SQL_Latin1_General_CP1_CI_AS AS [DataType]
          , JSON_VALUE([c].[value], '$.value')COLLATE SQL_Latin1_General_CP1_CI_AS AS [Value]
        INTO [#param2]
        FROM OPENJSON(@ARMTemplateForFactory, '$.resources') AS [b]
        CROSS APPLY OPENJSON([b].[value], '$.properties') AS [c]
        WHERE JSON_VALUE([b].[value], '$.type') = 'Microsoft.DataFactory/factories/globalparameters' ;

        SELECT
            [a].[ParameterName]
          , [a].[DataType]
          , [a].[Metadata]
          , [a].[DefaultValue]
        INTO [ADF].[Parameters]
        FROM [#param1] AS [a]
        WHERE [a].[ParameterName] NOT LIKE 'default_properties_%_value' ;

        ALTER TABLE [ADF].[Parameters] ADD CONSTRAINT [Parameters_PKC] PRIMARY KEY CLUSTERED( [ParameterName] ) ;

        SELECT
            [a].[ParameterName]
          , [a].[DataType]
          , [b].[DefaultValue] AS [Value]
        INTO [ADF].[GlobalParameters]
        FROM [#param2] AS [a]
        LEFT JOIN [#param1] AS [b] ON '[parameters(''' + [b].[ParameterName] + ''')]' = [a].[Value] ;

        ALTER TABLE [ADF].[GlobalParameters] ADD CONSTRAINT [GlobalParameters_PKC] PRIMARY KEY CLUSTERED( [ParameterName] ) ;

        SELECT DISTINCT
               [t].[Pipeline]
             , ISNULL([t].[Activity], '') AS [Activity]
             , [t].[ActivityType]
             , [t].[TimeOut]
             , [t].[Retry]
             , [t].[RetryIntervalInSeconds]
             , [t].[SecureOutput]
             , [t].[SecureInput]
             , [t].[DataIntegrationUnits]
             , [t].[EnableStaging]
        INTO [ADF].[ActivityProperties]
        FROM( SELECT
                  [t].[Pipeline]
                , [t].[L1_Activity] AS [Activity]
                , [t].[L1_ActivityType] AS [ActivityType]
                , [t].[TimeOut]
                , [t].[Retry]
                , [t].[RetryIntervalInSeconds]
                , [t].[SecureOutput]
                , [t].[SecureInput]
                , [t].[DataIntegrationUnits]
                , [t].[EnableStaging]
              FROM [#Parent] AS [t]
              WHERE [t].[Retry] IS NOT NULL
              UNION ALL
              SELECT
                  [t].[Pipeline]
                , [t].[L2_Activity] AS [Activity]
                , [t].[L2_ActivityType] AS [ActivityType]
                , [t].[TimeOut]
                , [t].[Retry]
                , [t].[RetryIntervalInSeconds]
                , [t].[SecureOutput]
                , [t].[SecureInput]
                , [t].[DataIntegrationUnits]
                , [t].[EnableStaging]
              FROM [#L2] AS [t]
              WHERE [t].[Retry] IS NOT NULL
              UNION ALL
              SELECT
                  [t].[Pipeline]
                , [t].[L3_Activity] AS [Activity]
                , [t].[L3_ActivityType] AS [ActivityType]
                , [t].[TimeOut]
                , [t].[Retry]
                , [t].[RetryIntervalInSeconds]
                , [t].[SecureOutput]
                , [t].[SecureInput]
                , [t].[DataIntegrationUnits]
                , [t].[EnableStaging]
              FROM [#L3] AS [t]
              WHERE [t].[Retry] IS NOT NULL ) AS [t] ;

        ALTER TABLE [ADF].[ActivityProperties] ADD CONSTRAINT [ActivityProperties_PKC] PRIMARY KEY CLUSTERED( [Pipeline], [Activity] ) ;

        ;WITH [L2]
        AS ( SELECT *
             FROM [#L2] AS [a]
             WHERE NOT EXISTS ( SELECT * FROM [#L3] AS [b] WHERE [a].[Pipeline] = [b].[Pipeline] AND [a].[L1_Id] = [b].[L1_Id] AND [a].[L2_Id] = [b].[L2_Id] ))
            , [l1]
        AS ( SELECT * FROM [#Parent] AS [a] WHERE NOT EXISTS ( SELECT * FROM [#L2] AS [b] WHERE [a].[Pipeline] = [b].[Pipeline] AND [a].[L1_Id] = [b].[L1_Id] ))
        SELECT
            [t].[Pipeline]
          , 0 AS [ActivitiesCnt]
          , ISNULL([t].[L1_Activity], '') AS [L1_Activity]
          , ISNULL([t].[L1_ActivityType], '') AS [L1_ActivityType]
          , ISNULL(CAST([t].[L1_Id] AS INT), 0) AS [L1_Id]
          , 0 AS [L1_ChildrenCnt]
          , [t].[L1_SwitchExpressionValue]
          , CAST([t].[L1_SwitchExpressionType] AS VARCHAR(128)) AS [L1_SwitchExpressionType]
          , [t].[L1_IFExpressionValue]
          , CAST([t].[L1_IFExpressionType] AS VARCHAR(128)) AS [L1_IFExpressionType]
          , [t].[L2_Activity]
          , [t].[L2_ActivityType]
          , CAST([t].[L2_Id] AS INT) AS [L2_Id]
          , 0 AS [L2_ChildrenCnt]
          , [t].[L2_SwitchValue]
          , [t].[L2_IFCondition]
          , [t].[L2_SwitchExpressionValue]
          , CAST([t].[L2_SwitchExpressionType] AS VARCHAR(128)) AS [L2_SwitchExpressionType]
          , [t].[L2_IFExpressionValue]
          , CAST([t].[L2_IFExpressionType] AS VARCHAR(128)) AS [L2_IFExpressionType]
          , CAST([t].[L3_Id] AS INT) AS [L3_Id]
          , [t].[L3_Activity]
          , [t].[L3_ActivityType]
          , [t].[L3_SwitchValue]
          , [t].[L3_IFCondition]
        INTO [ADF].[PipeLineActivitiesPivot]
        FROM( SELECT
                  [t].[Pipeline]
                , [t].[L1_Id]
                , [t].[L1_Activity]
                , [t].[L1_ActivityType]
                , [t].[L2_Id]
                , [t].[L2_Activity]
                , [t].[L2_ActivityType]
                , NULL AS [L2_SwitchValue]
                , NULL AS [L1_SwitchExpressionValue]
                , NULL AS [L1_SwitchExpressionType]
                , NULL AS [L1_IFExpressionValue]
                , NULL AS [L1_IFExpressionType]
                , NULL AS [L2_IFCondition]
                , [t].[L3_Id]
                , [t].[L3_Activity]
                , [t].[L3_ActivityType]
                , [t].[L3_SwitchValue]
                , [t].[L2_SwitchExpressionValue]
                , [t].[L2_SwitchExpressionType]
                , [t].[L2_IFExpressionValue]
                , [t].[L2_IFExpressionType]
                , [t].[L3_IFCondition]
              FROM [#L3] AS [t]
              UNION ALL
              SELECT
                  [t].[Pipeline]
                , [t].[L1_Id]
                , [t].[L1_Activity]
                , [t].[L1_ActivityType]
                , NULL AS [L2_Id]
                , NULL AS [L2_Activity]
                , NULL AS [L2_ActivityType]
                , NULL AS [L2_SwitchValue]
                , NULL AS [L1_SwitchExpressionValue]
                , NULL AS [L1_SwitchExpressionType]
                , NULL AS [L1_IFExpressionValue]
                , NULL AS [L1_IFExpressionType]
                , NULL AS [L2_IFCondition]
                , NULL AS [L3_Id]
                , NULL AS [L3_Activity]
                , NULL AS [L3_ActivityType]
                , NULL AS [L3_SwitchValue]
                , NULL AS [L2_SwitchExpressionValue]
                , NULL AS [L2_SwitchExpressionType]
                , NULL AS [L2_IFExpressionValue]
                , NULL AS [L2_IFExpressionType]
                , NULL AS [L3_IFCondition]
              FROM [l1] AS [t]
              UNION ALL
              SELECT
                  [t].[Pipeline]
                , [t].[L1_Id]
                , [t].[L1_Activity]
                , [t].[L1_ActivityType]
                , [t].[L2_Id]
                , [t].[L2_Activity]
                , [t].[L2_ActivityType]
                , [t].[L2_SwitchValue]
                , [t].[L1_SwitchExpressionValue]
                , [t].[L1_SwitchExpressionType]
                , [t].[L1_IFExpressionValue]
                , [t].[L1_IFExpressionType]
                , [t].[L2_IFCondition]
                , NULL AS [L3_Id]
                , NULL AS [L3_Activity]
                , NULL AS [L3_ActivityType]
                , NULL AS [L3_SwitchValue]
                , NULL AS [L2_SwitchExpressionValue]
                , NULL AS [L2_SwitchExpressionType]
                , NULL AS [L2_IFExpressionValue]
                , NULL AS [L2_IFExpressionType]
                , NULL AS [L3_IFCondition]
              FROM [L2] AS [t] ) AS [t] ;

        UPDATE [f]
        SET
            [f].[L1_ChildrenCnt] = [L1_ChildrenCount_New]
          , [f].[L2_ChildrenCnt] = [L2_ChildrenCount_New]
        FROM( SELECT
                  [f].[L1_ChildrenCnt]
                , [f].[L2_ChildrenCnt]
                , ( SELECT COUNT(DISTINCT [b].[L2_Id])FROM [ADF].[PipeLineActivitiesPivot] AS [b] WHERE [b].[Pipeline] = [f].[Pipeline] AND [b].[L1_Id] = [f].[L1_Id] ) AS [L1_ChildrenCount_New]
                , ( SELECT COUNT(DISTINCT [b].[L3_Id])
                    FROM [ADF].[PipeLineActivitiesPivot] AS [b]
                    WHERE [b].[Pipeline] = [f].[Pipeline] AND [b].[L1_Id] = [f].[L1_Id] AND [b].[L2_Id] = [f].[L2_Id] ) AS [L2_ChildrenCount_New]
              FROM [ADF].[PipeLineActivitiesPivot] AS [f] ) AS [f] ;

        UPDATE
            [f]
        SET
            [f].[ActivitiesCnt] = [ActivitiesCount_New]
        FROM( SELECT
                  [f].[ActivitiesCnt]
                , ( SELECT COUNT(1)FROM [ADF].[PipeLineActivitiesPivot] AS [b] WHERE [b].[Pipeline] = [f].[Pipeline] ) + ( SELECT COUNT(DISTINCT [b].[L1_Id])FROM [ADF].[PipeLineActivitiesPivot] AS [b] WHERE [b].[Pipeline] = [f].[Pipeline] AND [b].[L1_ChildrenCnt] > 0 )
                  + ( SELECT COUNT(DISTINCT [b].[L2_Id])FROM [ADF].[PipeLineActivitiesPivot] AS [b] WHERE [b].[Pipeline] = [f].[Pipeline] AND [b].[L2_ChildrenCnt] > 0 ) AS [ActivitiesCount_New]
              FROM [ADF].[PipeLineActivitiesPivot] AS [f] ) AS [f] ;

        CREATE UNIQUE CLUSTERED INDEX [PipeLineActivitiesPivot] ON [ADF].[PipeLineActivitiesPivot]( [Pipeline], [L1_Id], [L2_Id], [L3_Id] ) ;

        DROP TABLE IF EXISTS [#temp_SDFS] ;

        WITH [parent]
        AS ( SELECT DISTINCT
                    [p].[Pipeline]
                  , 1 AS [Level]
                  , NULL AS [ParentActivity]
                  , NULL AS [ParentActivityType]
                  , NULL AS [ParentId]
                  , [p].[L1_Activity] AS [Activity]
                  , [p].[L1_ActivityType] AS [ActivityType]
                  , [p].[L1_Id] AS [Id]
                  , [p].[L1_SwitchExpressionValue] AS [SwitchExpressionValue]
                  , [p].[L1_SwitchExpressionType] AS [SwitchExpressionType]
                  , [p].[L1_IFExpressionValue] AS [IFExpressionValue]
                  , [p].[L1_IFExpressionType] AS [IFExpressionType]
                  , NULL AS [SwitchValue]
                  , NULL AS [IFCondition]
             FROM [ADF].[PipeLineActivitiesPivot] AS [p] )
           , [child1]
        AS ( SELECT DISTINCT
                    [c].[Pipeline]
                  , 2 AS [Level]
                  , [p].[Activity] AS [ParentActivity]
                  , [p].[ActivityType] AS [ParentActivityType]
                  , [p].[Id] AS [ParentId]
                  , [c].[L2_Activity] AS [Activity]
                  , [c].[L2_ActivityType] AS [ActivityType]
                  , [c].[L2_Id] AS [Id]
                  , [c].[L2_SwitchExpressionValue] AS [SwitchExpressionValue]
                  , [c].[L2_SwitchExpressionType] AS [SwitchExpressionType]
                  , [c].[L2_IFExpressionValue] AS [IFExpressionValue]
                  , [c].[L2_IFExpressionType] AS [IFExpressionType]
                  , [c].[L2_SwitchValue] AS [SwitchValue]
                  , [c].[L2_IFCondition] AS [IFCondition]
             FROM [parent] AS [p]
             INNER JOIN [ADF].[PipeLineActivitiesPivot] AS [c] ON [c].[Pipeline] = [p].[Pipeline] AND [c].[L1_Id] = [p].[Id] AND [c].[L1_ChildrenCnt] > 0 )
           , [child2]
        AS ( SELECT DISTINCT
                    [c].[Pipeline]
                  , 3 AS [Level]
                  , [p].[Activity] AS [ParentActivity]
                  , [p].[ActivityType] AS [ParentActivityType]
                  , [p].[Id] AS [ParentId]
                  , [c].[L3_Activity] AS [Activity]
                  , [c].[L3_ActivityType] AS [ActivityType]
                  , [c].[L3_Id] AS [Id]
                  , NULL AS [SwitchExpressionValue]
                  , NULL AS [SwitchExpressionType]
                  , NULL AS [IFExpressionValue]
                  , NULL AS [IFExpressionType]
                  , [c].[L3_SwitchValue] AS [SwitchValue]
                  , [c].[L3_IFCondition] AS [IFCondition]
             FROM [child1] AS [p]
             INNER JOIN [ADF].[PipeLineActivitiesPivot] AS [c] ON [c].[Pipeline] = [p].[Pipeline] AND [c].[L1_Id] = [p].[ParentId] AND [c].[L2_Id] = [p].[Id] AND [c].[L2_ChildrenCnt] > 0 )
        SELECT
            [p].[Pipeline]
          , [p].[Level]
          , [p].[ParentActivity]
          , [p].[ParentActivityType]
          , [p].[ParentId]
          , [p].[Activity]
          , [p].[ActivityType]
          , [p].[Id]
          , [p].[SwitchExpressionValue]
          , [p].[SwitchExpressionType]
          , [p].[IFExpressionValue]
          , [p].[IFExpressionType]
          , [p].[SwitchValue]
          , [p].[IFCondition]
        INTO [#temp_SDFS]
        FROM [parent] AS [p]
        UNION ALL
        SELECT
            [p].[Pipeline]
          , [p].[Level]
          , [p].[ParentActivity]
          , [p].[ParentActivityType]
          , [p].[ParentId]
          , [p].[Activity]
          , [p].[ActivityType]
          , [p].[Id]
          , [p].[SwitchExpressionValue]
          , [p].[SwitchExpressionType]
          , [p].[IFExpressionValue]
          , [p].[IFExpressionType]
          , [p].[SwitchValue]
          , [p].[IFCondition]
        FROM [child1] AS [p]
        UNION ALL
        SELECT
            [p].[Pipeline]
          , [p].[Level]
          , [p].[ParentActivity]
          , [p].[ParentActivityType]
          , [p].[ParentId]
          , [p].[Activity]
          , [p].[ActivityType]
          , [p].[Id]
          , [p].[SwitchExpressionValue]
          , [p].[SwitchExpressionType]
          , [p].[IFExpressionValue]
          , [p].[IFExpressionType]
          , [p].[SwitchValue]
          , [p].[IFCondition]
        FROM [child2] AS [p] ;

        CREATE UNIQUE CLUSTERED INDEX [temp] ON [#temp_SDFS]( [Pipeline], [Level], [ParentId], [Id] ) ;

        DROP TABLE IF EXISTS [#temp_SDFS2] ;

        SELECT
            [a].[Pipeline]
          , ISNULL(( SELECT COUNT(1)FROM [#temp_SDFS] AS [b] WHERE [a].[Pipeline] = [b].[Pipeline] ), 0) AS [PipelineActivitiesCnt]
          , [a].[Level]
          , ISNULL(( SELECT COUNT(DISTINCT [b].[Id])FROM [#temp_SDFS] AS [b] WHERE [a].[Pipeline] = [b].[Pipeline] AND [a].[Level] + 1 = [b].[Level] AND [a].[Id] = [b].[ParentId] ), 0) AS [ChildrenCnt]
          , [a].[ParentId]
          , [a].[Id]
          , [a].[Activity]
          , [a].[ActivityType]
          , [a].[ParentActivity]
          , [a].[ParentActivityType]
          , [a].[SwitchExpressionValue]
          , [a].[SwitchExpressionType]
          , [a].[IFExpressionValue]
          , [a].[IFExpressionType]
          , [a].[SwitchValue]
          , [a].[IFCondition]
        INTO [ADF].[PipeLineActivities]
        FROM [#temp_SDFS] AS [a] ;

        CREATE UNIQUE CLUSTERED INDEX [PipeLineActivities] ON [ADF].[PipeLineActivities]( [Pipeline], [Level], [ParentId], [Id] ) ;

        SELECT
            ISNULL(CAST(REPLACE(REPLACE(JSON_VALUE([b].[value], '$.name'), '[concat(parameters(''factoryName''), ''/', ''), ''')]', '') AS VARCHAR(128)), '') AS [IntegrationRuntime]
          , CAST(JSON_VALUE([b].[value], '$.properties.type') AS VARCHAR(128)) AS [Type]
          , CAST([d].[DependencyType] AS VARCHAR(128)) AS [DependencyType]
          , CAST([d].[DependencyName] AS VARCHAR(128)) AS [DependencyName]
        INTO [ADF].[IntegrationRunTimes]
        FROM OPENJSON(@ARMTemplateForFactory, '$.resources') AS [b]
        OUTER APPLY OPENJSON(JSON_QUERY([b].[value], '$.dependsOn')) AS [c]
        OUTER APPLY( SELECT
                         MAX(CASE WHEN [d].[ordinal] = 1 THEN [d].[value] END) AS [DependencyType]
                       , MAX(CASE WHEN [d].[ordinal] = 2 THEN [d].[value] END) AS [DependencyName]
                     FROM STRING_SPLIT(REPLACE(REPLACE([c].[value], '[concat(variables(''factoryId''), ''/', ''), ''')]', ''), '/', 1) AS [d] ) AS [d]
        WHERE JSON_VALUE([b].[value], '$.type') = 'Microsoft.DataFactory/factories/integrationRuntimes' ;

        ALTER TABLE [ADF].[IntegrationRunTimes] ADD CONSTRAINT [IntegrationRunTimes_PKC] PRIMARY KEY CLUSTERED( [IntegrationRuntime] ) ;

        SELECT
            ISNULL(CAST(REPLACE(REPLACE(JSON_VALUE([b].[value], '$.name'), '[concat(parameters(''factoryName''), ''/', ''), ''')]', '') AS VARCHAR(128)), '') AS [LinkedService]
          , CAST(JSON_VALUE([b].[value], '$.properties.type') AS VARCHAR(128)) AS [Type]
          , CAST(JSON_VALUE([b].[value], '$.properties.connectVia.referenceName') AS VARCHAR(128)) AS [ConnectViaReferenceName]
          , CAST(JSON_VALUE([b].[value], '$.properties.connectVia.type') AS VARCHAR(128)) AS [ConnectViaType]
          , JSON_QUERY([b].[value], '$.properties.parameters') AS [Parameters]
          , NULLIF(JSON_QUERY([b].[value], '$.properties.annotations'), '[]') AS [Annotations]
          , JSON_QUERY([b].[value], '$.properties.typeProperties') AS [TypeProperties]
          , [b].[value] AS [Value]
        INTO [ADF].[LinkedServices]
        FROM OPENJSON(@ARMTemplateForFactory, '$.resources') AS [b]
        WHERE JSON_VALUE([b].[value], '$.type') = 'Microsoft.DataFactory/factories/linkedServices' ;

        ALTER TABLE [ADF].[LinkedServices] ADD CONSTRAINT [LinkedServices_PKC] PRIMARY KEY CLUSTERED( [LinkedService] ) ;

        SELECT
            -- 1. Standard Resource Identification
            ISNULL(CAST(REPLACE(REPLACE(JSON_VALUE([value], '$.name'), '[concat(parameters(''factoryName''), ''/', ''), ''')]', '') COLLATE SQL_Latin1_General_CP1_CI_AS AS VARCHAR(128)), '') AS [Dataset]

          -- 2. Dataset Specific Properties
          -- The specific type of the dataset (e.g., AzureSqlTable, DelimitedText)
          , ISNULL(CAST(JSON_VALUE([value], '$.properties.type') COLLATE SQL_Latin1_General_CP1_CI_AS AS VARCHAR(128)), '') AS [DatasetType]
          -- Linked Service Reference
          , ISNULL(CAST(JSON_VALUE([value], '$.properties.linkedServiceName.referenceName') COLLATE SQL_Latin1_General_CP1_CI_AS AS VARCHAR(128)), '') AS [LinkedService]
          -- Organizational Folder
          , CAST(JSON_VALUE([value], '$.properties.folder.name') COLLATE SQL_Latin1_General_CP1_CI_AS AS VARCHAR(128)) AS [Folder]
          -- Description (if available)
          , CAST(JSON_VALUE([value], '$.properties.description') COLLATE SQL_Latin1_General_CP1_CI_AS AS VARCHAR(8000)) AS [Description]
          -- 3. Complex Objects & Arrays (Kept as JSON as requested)
          -- "schema" is often an array; we keep it raw.
          , NULLIF(JSON_QUERY([value], '$.properties.schema'), '[]') AS [Schema]
          -- "parameters" is an object defining dynamic values; we keep it raw.
          , JSON_QUERY([value], '$.properties.parameters') AS [Parameters]
          -- "annotations" is an array of tags; we keep it raw.
          , NULLIF(JSON_QUERY([value], '$.properties.annotations'), '[]') AS [Annotations]
          -- "typeProperties" contains the variable attributes (TableName, FilePath, Delimiters, etc.)
          -- We keep this as a JSON object so no attributes are lost, regardless of dataset type.
          , JSON_QUERY([value], '$.properties.typeProperties') AS [TypeProperties]
          -- 4. Common extracted attributes for convenience (Nullable)
          -- These attempt to pull common values out of the typeProperties JSON for easier reading
          , CAST(COALESCE(JSON_VALUE([value], '$.properties.typeProperties.table'), JSON_VALUE([value], '$.properties.typeProperties.table.value')) COLLATE SQL_Latin1_General_CP1_CI_AS AS VARCHAR(128)) AS [TargetTable]
          , CAST(COALESCE(JSON_VALUE([value], '$.properties.typeProperties.schema'), JSON_VALUE([value], '$.properties.typeProperties.schema.value')) COLLATE SQL_Latin1_General_CP1_CI_AS AS VARCHAR(128)) AS [TargetSchema]
          , CAST(COALESCE(JSON_VALUE([value], '$.properties.typeProperties.location.container'), JSON_VALUE([value], '$.properties.typeProperties.location.fileSystem')) COLLATE SQL_Latin1_General_CP1_CI_AS AS VARCHAR(8000)) AS [FileLocation]
          , CAST(JSON_VALUE([value], '$.properties.typeProperties.location.folderPath') COLLATE SQL_Latin1_General_CP1_CI_AS AS VARCHAR(8000)) AS [FolderPath]
          , CAST(JSON_VALUE([value], '$.properties.typeProperties.location.fileName') COLLATE SQL_Latin1_General_CP1_CI_AS AS VARCHAR(8000)) AS [FileName]
        INTO [ADF].[DataSets]
        FROM( SELECT TOP 1 [ARMTemplateForFactory].[JSON] FROM [ADF].[ARMTemplateForFactory] ORDER BY [ARMTemplateForFactoryId] DESC ) AS [a]
        CROSS APPLY OPENJSON([a].[JSON], '$.resources')
        WHERE JSON_VALUE([value], '$.type') = 'Microsoft.DataFactory/factories/datasets' ;

        ALTER TABLE [ADF].[DataSets] ADD CONSTRAINT [DataSets_PKC] PRIMARY KEY CLUSTERED( [Dataset] ) ;

        SELECT
            ISNULL(CAST(REPLACE(REPLACE(JSON_VALUE([b].[value], '$.name'), '[concat(parameters(''factoryName''), ''/', ''), ''')]', '') AS VARCHAR(128)), '') AS [DataFlow]
          , CAST(JSON_VALUE([b].[value], '$.properties.type') AS VARCHAR(128)) AS [Type]
          , ISNULL(CAST([d].[DependencyType] AS VARCHAR(128)), '') AS [DependencyType]
          , ISNULL(CAST([d].[DependencyName] AS VARCHAR(128)), '') AS [DependencyName]
        INTO [ADF].[DataFlowDependencies]
        FROM OPENJSON(@ARMTemplateForFactory, '$.resources') AS [b]
        OUTER APPLY OPENJSON([b].[value], '$.dependsOn') AS [c]
        OUTER APPLY( SELECT
                         MAX(CASE WHEN [d].[ordinal] = 1 THEN [d].[value] END) AS [DependencyType]
                       , MAX(CASE WHEN [d].[ordinal] = 2 THEN [d].[value] END) AS [DependencyName]
                     FROM STRING_SPLIT(REPLACE(REPLACE([c].[value], '[concat(variables(''factoryId''), ''/', ''), ''')]', ''), '/', 1) AS [d] ) AS [d]
        WHERE JSON_VALUE([b].[value], '$.type') = 'Microsoft.DataFactory/factories/dataflows' ;

        ALTER TABLE [ADF].[DataFlowDependencies] ADD CONSTRAINT [DataFlowDependencies_PKC] PRIMARY KEY CLUSTERED( [DataFlow], [DependencyType], [DependencyName] ) ;

        SELECT
            [dd].[DataFlow]
          , MAX([dd].[Type]) AS [Type]
          , COUNT([dd].[DependencyName]) AS [DependencyCnt]
        INTO [ADF].[DataFlows]
        FROM [ADF].[DataFlowDependencies] AS [dd]
        GROUP BY [dd].[DataFlow] ;

        ALTER TABLE [ADF].[DataFlows] ADD CONSTRAINT [DataFlows_PKC] PRIMARY KEY CLUSTERED( [DataFlow] ) ;

        SELECT
            [k].[Pipeline]
          , ISNULL(CAST([z].[DependsOnType] AS VARCHAR(128)), '') AS [DependsOnType]
          , ISNULL(CAST([z].[DependsOnName] AS VARCHAR(128)), '') AS [DependsOnName]
        INTO [ADF].[PipeLineDependencies]
        FROM( SELECT DISTINCT [#temp_yyy].[Pipeline], [#temp_yyy].[dependsOn] FROM [#temp_yyy] ) AS [k]
        CROSS APPLY OPENJSON([k].[dependsOn]) AS [b]
        CROSS APPLY( SELECT
                         MAX(CASE WHEN [ordinal] = 1 THEN [z].[value] END) AS [DependsOnType]
                       , MAX(CASE WHEN [ordinal] = 2 THEN [z].[value] END) AS [DependsOnName]
                     FROM STRING_SPLIT(REPLACE(REPLACE([b].[value], '[concat(variables(''factoryId''), ''/', ''), ''')]', ''), '/', 1) AS [z] ) AS [z] ;

        ALTER TABLE [ADF].[PipeLineDependencies] ADD CONSTRAINT [PipeLineDependencies_PKC] PRIMARY KEY CLUSTERED( [Pipeline], [DependsOnType], [DependsOnName] ) ;

        SELECT
            [PipeLineDependencies].[Pipeline]
          , [PipeLineDependencies].[DependsOnType]
          , COUNT(1) AS [DependsOnCnt]
          , STRING_AGG(CAST('' AS VARCHAR(MAX)) + [PipeLineDependencies].[DependsOnName], ', ') WITHIN GROUP(ORDER BY [PipeLineDependencies].[DependsOnName]) AS [DependsOnList]
        INTO [ADF].[PipeLineDependenciesGrouped]
        FROM [ADF].[PipeLineDependencies]
        GROUP BY [PipeLineDependencies].[Pipeline]
               , [PipeLineDependencies].[DependsOnType] ;

        ALTER TABLE [ADF].[PipeLineDependenciesGrouped] ADD CONSTRAINT [PipeLineDependenciesGrouped_PKC] PRIMARY KEY CLUSTERED( [Pipeline], [DependsOnType] ) ;

        SELECT
            [t].[Pipeline]
          , ( SELECT MAX([pa].[ActivitiesCnt])FROM [ADF].[PipeLineActivitiesPivot] AS [pa] WHERE [pa].[Pipeline] = [t].[Pipeline] ) AS [PipelineActivityCnt]
          , COUNT([t].[ParameterName]) AS [ParameterCnt]
          , COUNT([t].[ParameterDefaultValue]) AS [ParameterWithValueCnt]
          , ( SELECT COUNT(1)FROM [ADF].[PipeLineDependencies] AS [pd] WHERE [pd].[Pipeline] = [t].[Pipeline] ) AS [DependsOnCnt]
        INTO [ADF].[PipeLines]
        FROM [#temp_yyy] AS [t]
        GROUP BY [t].[Pipeline] ;

        ALTER TABLE [ADF].[PipeLines] ADD CONSTRAINT [PipeLines_PKC] PRIMARY KEY CLUSTERED( [Pipeline] ) ;

        SELECT
            [#temp_yyy].[Pipeline]
          , COUNT([#temp_yyy].[ParameterName]) OVER ( PARTITION BY [#temp_yyy].[Pipeline] ) AS [ParameterCnt]
          , COUNT([#temp_yyy].[ParameterDefaultValue]) OVER ( PARTITION BY [#temp_yyy].[Pipeline] ) AS [ParameterWithValueCnt]
          , ISNULL([#temp_yyy].[ParameterName], '') AS [ParameterName]
          , [#temp_yyy].[ParameterType]
          , [#temp_yyy].[ParameterDefaultValue]
        INTO [ADF].[PipeLineParameters]
        FROM [#temp_yyy]
        WHERE [ParameterName] IS NOT NULL ;

        ALTER TABLE [ADF].[PipeLineParameters] ADD CONSTRAINT [PipeLineParameters_PKC] PRIMARY KEY CLUSTERED( [Pipeline], [ParameterName] ) ;

        SELECT
            [k].[TriggerName]
          , MAX([k].[RunTimeState]) AS [RunTimeState]
          , COUNT([k].[PipeLine]) AS [PipeLineCnt]
          , STRING_AGG(CAST('' AS VARCHAR(MAX)) + [k].[PipeLine], ', ') WITHIN GROUP(ORDER BY [k].[PipeLine]) AS [PipeLines]
          , MAX([k].[TriggerType]) AS [TriggerType]
          , MAX([k].[Frequency]) AS [Frequency]
          , CAST(MAX([k].[Interval]) AS INT) AS [Interval]
          , MAX([k].[StartTime]) AS [StartTime]
          , MAX([k].[TimeZone]) AS [TimeZone]
        INTO [ADF].[Triggers]
        FROM( SELECT DISTINCT
                     [#temp_xxx].[TriggerName]
                   , [#temp_xxx].[RunTimeState]
                   , [#temp_xxx].[PipeLine]
                   , [#temp_xxx].[TriggerType]
                   , [#temp_xxx].[Frequency]
                   , [#temp_xxx].[Interval]
                   , [#temp_xxx].[StartTime]
                   , [#temp_xxx].[TimeZone]
              FROM [#temp_xxx] ) AS [k]
        GROUP BY [k].[TriggerName] ;

        ALTER TABLE [ADF].[Triggers] ADD CONSTRAINT [Triggers_PKC] PRIMARY KEY CLUSTERED( [TriggerName] ) ;

        SELECT
            [k].[TriggerName]
          , [t].[RunTimeState]
          , COUNT(1) OVER ( PARTITION BY [k].[TriggerName] ) AS [ScheduleTimeCnt]
          , ISNULL(CAST(ROW_NUMBER() OVER ( PARTITION BY [k].[TriggerName] ORDER BY TIMEFROMPARTS([k].[Hours], [k].[Minutes], 0, 0, 0)) AS INT), 0) AS [ScheduleTimeId]
          , [t].[TriggerType]
          , [t].[Frequency]
          , [t].[Interval]
          , [t].[StartTime]
          , [t].[TimeZone]
          , TIMEFROMPARTS([k].[Hours], [k].[Minutes], 0, 0, 0) AS [ScheduleTime]
        INTO [ADF].[TriggerTimes]
        FROM( SELECT
                  [t].[TriggerName]
                , CAST([b].[value] AS INT) AS [Hours]
                , CAST([c].[value] AS INT) AS [Minutes]
              FROM( SELECT DISTINCT [t].[TriggerName], [t].[ScheduleHours], [t].[ScheduleMinutes] FROM [#temp_xxx] AS [t] ) AS [t]
              CROSS APPLY OPENJSON([t].[ScheduleHours]) AS [b]
              CROSS APPLY OPENJSON([t].[ScheduleMinutes]) AS [c] ) AS [k]
        INNER JOIN [ADF].[Triggers] AS [t] ON [t].[TriggerName] = [k].[TriggerName] ;

        ALTER TABLE [ADF].[TriggerTimes] ADD CONSTRAINT [TriggerTimes_PKC] PRIMARY KEY CLUSTERED( [TriggerName], [ScheduleTimeId] ) ;

        SELECT
            [k].[TriggerName]
          , [k].[RunTimeState]
          , COUNT(1) OVER ( PARTITION BY [k].[TriggerName] ) AS [ParameterCnt]
          , ISNULL([k].[ParameterName], '') AS [ParameterName]
          , [k].[ParameterType]
          , [k].[ParameterDefaultValue]
        INTO [ADF].[TriggerParameters]
        FROM( SELECT DISTINCT
                     [#temp_xxx].[TriggerName]
                   , [#temp_xxx].[RunTimeState]
                   , [#temp_xxx].[ParameterName]
                   , [#temp_xxx].[ParameterType]
                   , [#temp_xxx].[ParameterDefaultValue]
              FROM [#temp_xxx]
              WHERE [#temp_xxx].[ParameterName] IS NOT NULL ) AS [k] ;

        ALTER TABLE [ADF].[TriggerParameters] ADD CONSTRAINT [TriggerParameters_PKC] PRIMARY KEY CLUSTERED( [TriggerName], [ParameterName] ) ;

        SELECT
            [k].[TriggerName]
          , [k].[RunTimeState]
          , COUNT([k].[PipeLine]) OVER ( PARTITION BY [k].[TriggerName] ) AS [PipeLineCnt]
          , ISNULL([k].[PipeLine], '') AS [PipeLine]
        INTO [ADF].[TriggerPipeLines]
        FROM( SELECT DISTINCT [#temp_xxx].[TriggerName], [#temp_xxx].[RunTimeState], [#temp_xxx].[PipeLine] FROM [#temp_xxx] WHERE [#temp_xxx].[PipeLine] <> '' ) AS [k] ;

        ALTER TABLE [ADF].[TriggerPipeLines] ADD CONSTRAINT [TriggerPipeLines_PKC] PRIMARY KEY CLUSTERED( [TriggerName], [PipeLine] ) ;

        DROP TABLE IF EXISTS [#main_xx] ;

        SELECT DISTINCT
               [t].[Pipeline]
             , [t].[Id]
             , [t].[Activity]
             , [t].[ActivityType]
             , [TypeProperties]
        INTO [#main_xx]
        FROM( SELECT
                  [t].[Pipeline]
                , [t].[L1_Id] AS [Id]
                , [t].[L1_Activity] AS [Activity]
                , [t].[L1_ActivityType] AS [ActivityType]
                , [t].[L1_TypeProperties] AS [TypeProperties]
              FROM [#Parent] AS [t]
              UNION ALL
              SELECT
                  [t].[Pipeline]
                , [t].[L2_Id] AS [Id]
                , [t].[L2_Activity] AS [Activity]
                , [t].[L2_ActivityType] AS [ActivityType]
                , [t].[L2_TypeProperties] AS [TypeProperties]
              FROM [#L2] AS [t]
              UNION ALL
              SELECT
                  [t].[Pipeline]
                , [t].[L3_Id] AS [Id]
                , [t].[L3_Activity] AS [Activity]
                , [t].[L3_ActivityType] AS [ActivityType]
                , [t].[L3_TypeProperties] AS [TypeProperties]
              FROM [#L3] AS [t] ) AS [t] ;

        SELECT
            [k].[Pipeline]
          , ISNULL(CAST([k].[Activity] AS VARCHAR(256)), '') AS [Activity]
          , ISNULL(CAST([k].[ActivityType] AS VARCHAR(256)), '') AS [ActivityType]
          , [k].[ActivityId]
          , [k].[Query]
        INTO [ADF].[ActivityQueries]
        FROM( SELECT
                  [a].[Pipeline]
                , [a].[ActivityType]
                , [a].[Activity]
                , ISNULL(CAST(ROW_NUMBER() OVER ( PARTITION BY [a].[Pipeline] ORDER BY [a].[Id], [a].[Activity] ) AS INT), 0) AS [ActivityId]
                , JSON_VALUE([a].[TypeProperties], '$.source.query.value') AS [Query]
              FROM [#main_xx] AS [a]
              WHERE [a].[ActivityType] = 'Lookup' AND JSON_VALUE([a].[TypeProperties], '$.source.query.value') IS NOT NULL ) AS [k] ;

        ALTER TABLE [ADF].[ActivityQueries] ADD CONSTRAINT [ActivityQueries_PKC] PRIMARY KEY CLUSTERED( [Pipeline], [Activity], [ActivityId] ) ;

        SELECT
            [a].[Pipeline]
          , ISNULL(CAST([a].[Activity] AS VARCHAR(256)), '') AS [Activity]
          , ISNULL(CAST([a].[ActivityType] AS VARCHAR(256)), '') AS [ActivityType]
          , ISNULL(CAST(ROW_NUMBER() OVER ( PARTITION BY [a].[Pipeline] ORDER BY [a].[Id], [a].[Activity] ) AS INT), 0) AS [ActivityId]
          , CAST(JSON_VALUE([a].[TypeProperties], '$.variableName') AS VARCHAR(256)) AS [VariableName]
          , JSON_VALUE([a].[TypeProperties], '$.value.value') AS [Query]
        INTO [ADF].[VariableAssigments]
        FROM [#main_xx] AS [a]
        WHERE [a].[ActivityType] = 'SetVariable' ;

        ALTER TABLE [ADF].[VariableAssigments] ADD CONSTRAINT [VariableAssigments_PKC] PRIMARY KEY CLUSTERED( [Pipeline], [Activity] ) ;

        SELECT
            [k].[Pipeline]
          , ISNULL(CAST([k].[Activity] AS VARCHAR(128)), '') AS [Activity]
          , ISNULL(CAST([k].[ActivityType] AS VARCHAR(128)), '') AS [ActivityType]
          , ISNULL(CAST(QUOTENAME(ISNULL(TRIM(PARSENAME([k].[StoredProcedureName], 2)), 'dbo')) + '.' + QUOTENAME(TRIM(PARSENAME([k].[StoredProcedureName], 1))) AS VARCHAR(256)), '') AS [StoredProcedureName]
          , [k].[StoredProcedureId]
          , ISNULL(CAST([d].[StoredProcedureParamId] AS INT), 0) AS [StoredProcedureParamId]
          , CAST([d].[Key] AS VARCHAR(128)) COLLATE SQL_Latin1_General_CP1_CI_AS AS [ParameterName]
          , CAST(JSON_VALUE([d].[Value], '$.type') AS VARCHAR(128)) COLLATE SQL_Latin1_General_CP1_CI_AS AS [ParameterDataType]
          , CAST(JSON_VALUE([d].[Value], '$.value.type') AS VARCHAR(128)) COLLATE SQL_Latin1_General_CP1_CI_AS AS [ParameterType]
          , CAST(ISNULL(JSON_VALUE([d].[Value], '$.value'), JSON_VALUE([d].[Value], '$.value.value')) AS VARCHAR(MAX)) COLLATE SQL_Latin1_General_CP1_CI_AS AS [ParameterValue]
        INTO [ADF].[StoredProcedureOrLookupParameters]
        FROM( SELECT
                  [a].[Pipeline]
                , [a].[ActivityType]
                , [a].[Activity]
                , ISNULL(CAST(ROW_NUMBER() OVER ( PARTITION BY [a].[Pipeline] ORDER BY [a].[Id], [a].[Activity] ) AS INT), 0) AS [StoredProcedureId]
                , [a].[TypeProperties]
                , ISNULL(CAST(REPLACE(ISNULL(JSON_VALUE([a].[TypeProperties], '$.storedProcedureName'), JSON_VALUE([a].[TypeProperties], '$.storedProcedureName.value')), '[[', '[') AS VARCHAR(256)), '') AS [StoredProcedureName]
              FROM [#main_xx] AS [a]
              WHERE [a].[ActivityType] <> 'Lookup' AND ISNULL(JSON_VALUE([a].[TypeProperties], '$.storedProcedureName'), JSON_VALUE([a].[TypeProperties], '$.storedProcedureName.value')) IS NOT NULL ) AS [k]
        OUTER APPLY( SELECT ROW_NUMBER() OVER ( ORDER BY( SELECT 0 )) AS [StoredProcedureParamId], * FROM OPENJSON([TypeProperties], '$.storedProcedureParameters') AS [d] ) AS [d]
        UNION ALL
        SELECT
            [k].[Pipeline]
          , ISNULL(CAST([k].[Activity] AS VARCHAR(128)), '') AS [Activity]
          , ISNULL(CAST([k].[ActivityType] AS VARCHAR(128)), '') AS [ActivityType]
          , ISNULL(CAST(QUOTENAME(ISNULL(TRIM(PARSENAME([k].[StoredProcedureName], 2)), 'dbo')) + '.' + QUOTENAME(TRIM(PARSENAME([k].[StoredProcedureName], 1))) AS VARCHAR(256)), '') AS [StoredProcedureName]
          , [k].[StoredProcedureId]
          , ISNULL(CAST([d].[StoredProcedureParamId] AS INT), 0) AS [StoredProcedureParamId]
          , CAST([d].[Key] AS VARCHAR(128)) COLLATE SQL_Latin1_General_CP1_CI_AS AS [ParameterName]
          , CAST(JSON_VALUE([d].[Value], '$.type') AS VARCHAR(128)) COLLATE SQL_Latin1_General_CP1_CI_AS AS [ParameterDataType]
          , CAST(JSON_VALUE([d].[Value], '$.value.type') AS VARCHAR(128)) COLLATE SQL_Latin1_General_CP1_CI_AS AS [ParameterType]
          , CAST(ISNULL(JSON_VALUE([d].[Value], '$.value'), JSON_VALUE([d].[Value], '$.value.value')) AS VARCHAR(MAX)) COLLATE SQL_Latin1_General_CP1_CI_AS AS [ParameterValue]
        FROM( SELECT
                  [a].[Pipeline]
                , [a].[ActivityType]
                , [a].[Activity]
                , ISNULL(CAST(ROW_NUMBER() OVER ( PARTITION BY [a].[Pipeline] ORDER BY [a].[Id], [a].[Activity] ) AS INT), 0) AS [StoredProcedureId]
                , [a].[TypeProperties]
                , CAST(ISNULL(REPLACE(ISNULL(JSON_VALUE([a].[TypeProperties], '$.source.sqlReaderStoredProcedureName'), JSON_VALUE([a].[TypeProperties], '$.source.sqlReaderStoredProcedureName.value')), '[[', '['), '') AS VARCHAR(256)) AS [StoredProcedureName]
              FROM [#main_xx] AS [a]
              WHERE [a].[ActivityType] = 'Lookup' AND ISNULL(JSON_VALUE([a].[TypeProperties], '$.source.sqlReaderStoredProcedureName'), JSON_VALUE([a].[TypeProperties], '$.source.sqlReaderStoredProcedureName.value')) IS NOT NULL ) AS [k]
        OUTER APPLY( SELECT ROW_NUMBER() OVER ( ORDER BY( SELECT 0 )) AS [StoredProcedureParamId], * FROM OPENJSON([TypeProperties], '$.source.storedProcedureParameters') AS [d] ) AS [d] ;

        ALTER TABLE [ADF].[StoredProcedureOrLookupParameters]
        ADD CONSTRAINT [StoredProcedureOrLookupParameters_PKC] PRIMARY KEY CLUSTERED( [Pipeline], [Activity], [StoredProcedureId], [StoredProcedureParamId] ) ;

        SELECT
            [k].[Pipeline]
          , [k].[Activity]
          , [k].[ActivityType]
          , ISNULL(CAST(ROW_NUMBER() OVER ( PARTITION BY [k].[Pipeline] ORDER BY [k].[Id], [k].[Activity] ) AS INT), 0) AS [QueryId]
          , [k].[Query]
          , [k].[Type]
        INTO [ADF].[SQLQueries]
        FROM( SELECT
                  ISNULL([k].[Pipeline], '') AS [Pipeline]
                , ISNULL([k].[Activity], '') AS [Activity]
                , [k].[Id]
                , [k].[ActivityType]
                , ISNULL(CAST(ROW_NUMBER() OVER ( PARTITION BY [k].[Pipeline] ORDER BY [k].[Id], [k].[Activity] ) AS INT), 0) AS [QueryId]
                , [k].[Query]
                , [k].[Type]
              FROM( SELECT
                        *
                      , ISNULL(JSON_VALUE([a].[TypeProperties], '$.source.sqlReaderQuery'), JSON_VALUE([a].[TypeProperties], '$.source.sqlReaderQuery.value')) AS [Query]
                      , JSON_VALUE([a].[TypeProperties], '$.source.sqlReaderQuery.type') AS [Type]
                    FROM [#main_xx] AS [a] ) AS [k]
              WHERE [k].[Query] IS NOT NULL
              UNION ALL
              SELECT
                  ISNULL([k].[Pipeline], '') AS [Pipeline]
                , ISNULL([k].[Activity], '') AS [Activity]
                , [k].[Id]
                , [k].[ActivityType]
                , ISNULL(CAST(ROW_NUMBER() OVER ( PARTITION BY [k].[Pipeline] ORDER BY [k].[Id], [k].[Activity] ) AS INT), 0) AS [QueryId]
                , [d].[value] AS [Query]
                , [d].[type] AS [Type]
              FROM [#main_xx] AS [k]
              CROSS APPLY OPENJSON([k].[TypeProperties], '$.scripts')
                          WITH( [type] VARCHAR(128), [text] NVARCHAR(MAX) AS JSON ) AS [c]
              CROSS APPLY OPENJSON([c].[text])
                          WITH( [value] VARCHAR(MAX), [type] NVARCHAR(128)) AS [d] ) AS [k] ;

        ALTER TABLE [ADF].[SQLQueries] ADD CONSTRAINT [SQLQueries_PKC] PRIMARY KEY CLUSTERED( [Pipeline], [Activity] ) ;

        SELECT
            [k].[Pipeline]
          , ISNULL([k].[StoredProcedureName], '') AS [StoredProcedureName]
          , [k].[StoredProcedureId]
          , [b].[ActivityTypeList]
          , [b].[StoredProcedureCnt]
          , [k].[StoredProcedureParamCnt]
          , [k].[StoredProcedureParamNotNullCnt]
        INTO [ADF].[StoredProcedureOrLookup]
        FROM( SELECT
                  [k].[Pipeline]
                , [k].[StoredProcedureName]
                , [k].[StoredProcedureId]
                , COUNT([k].[StoredProcedureParamId]) AS [StoredProcedureParamCnt]
                , COUNT([k].[ParameterValue]) AS [StoredProcedureParamNotNullCnt]
              FROM [ADF].[StoredProcedureOrLookupParameters] AS [k]
              GROUP BY [k].[Pipeline]
                     , [k].[StoredProcedureName]
                     , [k].[StoredProcedureId] ) AS [k]
        LEFT JOIN( SELECT
                       [k].[Pipeline]
                     , [k].[StoredProcedureName]
                     , [k].[StoredProcedureId]
                     , STRING_AGG(CAST('' AS NVARCHAR(MAX)) + [k].[ActivityType], ', ') WITHIN GROUP(ORDER BY [k].[ActivityType]) AS [ActivityTypeList]
                     , COUNT(1) AS [StoredProcedureCnt]
                   FROM( SELECT DISTINCT [k].[Pipeline], [k].[ActivityType], [k].[StoredProcedureName], [k].[StoredProcedureId] FROM [ADF].[StoredProcedureOrLookupParameters] AS [k] ) AS [k]
                   GROUP BY [k].[Pipeline]
                          , [k].[StoredProcedureName]
                          , [k].[StoredProcedureId] ) AS [b] ON [k].[Pipeline] = [b].[Pipeline] AND [k].[StoredProcedureName] = [b].[StoredProcedureName] AND [k].[StoredProcedureId] = [b].[StoredProcedureId] ;

        ALTER TABLE [ADF].[StoredProcedureOrLookup] ADD CONSTRAINT [StoredProcedureOrLookup_PKC] PRIMARY KEY CLUSTERED( [Pipeline], [StoredProcedureName], [StoredProcedureId] ) ;

        SELECT
            ISNULL(CAST(REPLACE(REPLACE(JSON_VALUE([b].[value], '$.name'), '[concat(parameters(''factoryName''), ''/', ''), ''')]', '') AS VARCHAR(128)), '') AS [Pipeline]
          , [b].[value]
        INTO [#pipelines]
        FROM OPENJSON(@ARMTemplateForFactory, '$.resources') AS [b]
        WHERE JSON_VALUE([b].[value], '$.type') = 'Microsoft.DataFactory/factories/pipelines' ;

        WITH [RecursiveJson]
        AS ( SELECT
                 [p].[Pipeline]
               , [p].[Value] AS [JsonData]
               , CAST(0 AS INT) AS [Level]
             FROM [#pipelines] AS [p]
             UNION ALL
             SELECT
                 [p].[Pipeline]
               , CAST(CASE WHEN [j].[Type] = 4 THEN [j].[Value] -- Object
                          WHEN [j].[Type] = 5 THEN [j].[Value] -- Array
                          ELSE NULL
                      END AS NVARCHAR(MAX)) AS [JsonData]
               , CAST([p].[Level] + 1 AS INT) AS [Level]
             FROM [RecursiveJson] AS [p]
             CROSS APPLY OPENJSON([p].[JsonData]) AS [j]
             WHERE( [j].[Type] = 4 OR [j].[Type] = 5 ) -- Only process objects or arrays for further recursion
        )
        SELECT
            [rj].[Pipeline]
          , [j].[Key] AS [PropertyName]
          , [j].[Value] AS [PropertyValue]
          , [j].[Type] AS [PropertyType]
          , [rj].[Level]
        INTO [#recursive_resources]
        FROM [RecursiveJson] AS [rj]
        CROSS APPLY OPENJSON([rj].[JsonData]) AS [j]
        WHERE [j].[Key] = 'activities'
        OPTION( MAXRECURSION 500 ) ;

        SELECT
            ISNULL(CAST([r].[Pipeline] AS VARCHAR(128)), '') AS [Pipeline]
          , ISNULL(CAST(JSON_VALUE([b].[value], '$.name') AS VARCHAR(128)), '') AS [ActivityName]
          , JSON_VALUE([b].[value], '$.type') AS [ActivityType]
          , [r].[Level]
          , ISNULL(CAST(ROW_NUMBER() OVER ( PARTITION BY [r].[Pipeline] ORDER BY [r].[Level], ( SELECT 0 )) AS INT), 0) AS [ActivityId]
          , [b].[value] AS [ActivityValue]
        INTO [ADF].[PipeLineAllActivities]
        FROM [#recursive_resources] AS [r]
        CROSS APPLY OPENJSON([r].[PropertyValue]) AS [b] ;

        ALTER TABLE [ADF].[PipeLineAllActivities] ADD CONSTRAINT [PipeLineAllActivities_PKC] PRIMARY KEY CLUSTERED( [Pipeline], [ActivityId] ) ;

        SELECT
            [A].[Pipeline]
          , [A].[ActivityName]
          , [A].[Level]
          , [A].[ActivityType]
          , [A].[InputOrOutput]
          , [A].[InputOutputId]
          , [A].[ReferenceName]
          , [A].[ReferenceType]
          , [DS].[LinkedService]
          , [A].[ReferenceValue]
        INTO [ADF].[ActivityReferences]
        FROM( SELECT
                  ISNULL(CAST([r].[Pipeline] AS VARCHAR(128)), '') AS [Pipeline]
                , ISNULL(CAST(JSON_VALUE([b].[value], '$.name') AS VARCHAR(128)), '') AS [ActivityName]
                , [r].[Level]
                , JSON_VALUE([b].[value], '$.type') AS [ActivityType]
                , 'Input' AS [InputOrOutput]
                , ISNULL(CAST(ROW_NUMBER() OVER ( PARTITION BY [r].[Pipeline], JSON_VALUE([b].[value], '$.name')ORDER BY( SELECT 0 )) AS INT), 0) AS [InputOutputId]
                , JSON_VALUE([c].[value], '$.referenceName') AS [ReferenceName]
                , JSON_VALUE([c].[value], '$.type') AS [ReferenceType]
                , [c].[value] AS [ReferenceValue]
              FROM [#recursive_resources] AS [r]
              CROSS APPLY OPENJSON([r].[PropertyValue]) AS [b]
              CROSS APPLY OPENJSON([b].[value], '$.inputs') AS [c]
              UNION ALL
              SELECT
                  ISNULL(CAST([r].[Pipeline] AS VARCHAR(128)), '') AS [Pipeline]
                , ISNULL(CAST(JSON_VALUE([b].[value], '$.name') AS VARCHAR(128)), '') AS [ActivityName]
                , [r].[Level]
                , JSON_VALUE([b].[value], '$.type') AS [ActivityType]
                , 'Output' AS [InputOrOutput]
                , ISNULL(CAST(ROW_NUMBER() OVER ( PARTITION BY [r].[Pipeline], JSON_VALUE([b].[value], '$.name')ORDER BY( SELECT 0 )) AS INT), 0) AS [InputOutputId]
                , JSON_VALUE([c].[value], '$.referenceName') AS [ReferenceName]
                , JSON_VALUE([c].[value], '$.type') AS [ReferenceType]
                , [c].[value] AS [ReferenceValue]
              FROM [#recursive_resources] AS [r]
              CROSS APPLY OPENJSON([r].[PropertyValue]) AS [b]
              CROSS APPLY OPENJSON([b].[value], '$.outputs') AS [c] ) AS [A]
        LEFT JOIN [ADF].[DataSets] AS [DS] ON [DS].[Dataset] = [A].[ReferenceName] ;

        ALTER TABLE [ADF].[ActivityReferences] ADD CONSTRAINT [ActivityReferences_PKC] PRIMARY KEY CLUSTERED( [Pipeline], [ActivityName], [InputOrOutput], [InputOutputId] ) ;

        UPDATE [a]
        SET
            [a].[ImportSuccess] = 1
          , [a].[UpdatedDateTime] = GETDATE()
        FROM [ADF].[ARMTemplateForFactory] AS [a]
        WHERE [a].[ARMTemplateForFactoryId] = @ARMTemplateForFactoryId ;

        IF @UseTransaction = 1
            COMMIT ;

        DROP TABLE IF EXISTS [##Columns] ;

        SELECT
            QUOTENAME([s].[name]) + '.' + QUOTENAME([t].[name]) AS [FQN]
          , [s].[name] AS [SchemaName]
          , [t].[name] AS [ObjectName]
          , [c].[name] AS [ColumnName]
          , [c].[column_id] AS [ColumnId]
          , [ic].[key_ordinal] AS [PKOrdinal]
          , [ys].[name] AS [DataType]
          , [c].[collation_name] AS [CollationName]
          , [t].[type] AS [ObjectType]
          , CAST(CASE WHEN [ys].[name] = 'timestamp' THEN 'rowversion'
                     WHEN [ys].[name] IN ('char', 'varchar') THEN CONCAT([ys].[name], '(', CASE WHEN [c].[max_length] = -1 THEN 'MAX' ELSE CAST([c].[max_length] AS VARCHAR) END, ')', CASE WHEN [c].[collation_name] <> [d].[collation_name] THEN CONCAT(' COLLATE ', [c].[collation_name]) ELSE '' END)
                     WHEN [ys].[name] IN ('nchar', 'nvarchar') THEN
                         CONCAT([ys].[name], '(', CASE WHEN [c].[max_length] = -1 THEN 'MAX' ELSE CAST([c].[max_length] / 2 AS VARCHAR) END, ')', CASE WHEN [c].[collation_name] <> [d].[collation_name] THEN CONCAT(' COLLATE ', [c].[collation_name]) ELSE '' END)
                     WHEN [ys].[name] IN ('binary', 'varbinary') THEN CONCAT([ys].[name], '(', CASE WHEN [c].[max_length] = -1 THEN 'MAX' ELSE CAST([c].[max_length] AS VARCHAR) END, ')')
                     WHEN [ys].[name] IN ('bigint', 'int', 'smallint', 'tinyint') THEN [ys].[name]
                     WHEN [ys].[name] IN ('datetime2', 'time', 'datetimeoffset') THEN CONCAT([ys].[name], '(', [c].[scale], ')')
                     WHEN [ys].[name] IN ('numeric', 'decimal') THEN CONCAT([ys].[name], '(', [c].[precision], ', ', [c].[scale], ')')
                     ELSE [ys].[name]
                 END AS VARCHAR(256)) AS [SystemColumnDef]
          , [c].[is_identity] AS [IsIdentity]
          , [c].[is_nullable] AS [IsNullable]
          , [c].[max_length] AS [MaxLength]
          , [c].[precision] AS [Precision]
          , [c].[scale] AS [Scale]
          , [c].[is_computed] AS [IsComputed]
          , [t].[create_date] AS [ObjectCreateDate]
          , [t].[modify_date] AS [ObjectModifyDate]
          , [t].[object_id] AS [ObjectId]
        INTO [##Columns]
        FROM [sys].[tables] AS [t]
        INNER JOIN [sys].[schemas] AS [s] WITH( NOLOCK )ON [s].[schema_id] = [t].[schema_id]
        INNER JOIN [sys].[columns] AS [c] WITH( NOLOCK )ON [c].[object_id] = [t].[object_id]
        INNER JOIN [sys].[types] AS [ys] WITH( NOLOCK )ON [ys].[system_type_id] = [c].[system_type_id] AND [ys].[user_type_id] = [ys].[system_type_id]
        INNER JOIN [sys].[types] AS [yu] WITH( NOLOCK )ON [yu].[user_type_id] = [c].[user_type_id]
        LEFT JOIN [sys].[indexes] AS [i] WITH( NOLOCK )ON [i].[object_id] = [t].[object_id] AND [i].[index_id] = 1
        LEFT JOIN [sys].[index_columns] AS [ic] WITH( NOLOCK )ON [ic].[object_id] = [t].[object_id] AND [ic].[index_id] = [i].[index_id] AND [ic].[column_id] = [c].[column_id]
        LEFT JOIN [sys].[databases] AS [d] ON [d].[name] = DB_NAME()
        WHERE [s].[name] = 'ADF' AND [t].[create_date] > @GETDATE AND [t].[name] NOT IN ('ARMTemplateForFactory')
        OPTION( RECOMPILE ) ;

        SELECT @SQL = STRING_AGG([K].[SQL], '

')          WITHIN GROUP(ORDER BY [K].[SchemaName]
                                , [ObjectName])
        FROM( SELECT
                  [##Columns].[SchemaName]
                , [##Columns].[ObjectName]
                , CONCAT(
                      CAST('' AS VARCHAR(MAX)), 'DROP TABLE IF EXISTS ADF_HISTORY.', [##Columns].[ObjectName], '_', @ARMTemplateForFactoryId, ';

SELECT * INTO ADF_HISTORY.', [##Columns].[ObjectName], '_', @ARMTemplateForFactoryId, ' FROM ADF.', [##Columns].[ObjectName]
                    , '
ALTER TABLE ADF_HISTORY.' + [##Columns].[ObjectName] + '_' + CAST(@ARMTemplateForFactoryId AS VARCHAR) + ' ADD UNIQUE CLUSTERED('
                      + STRING_AGG(CAST('' AS VARCHAR(MAX)) + CASE WHEN [##Columns].[PKOrdinal] IS NOT NULL THEN [##Columns].[ColumnName] END, ', ') WITHIN GROUP(ORDER BY [##Columns].[PKOrdinal]) + ');') AS [SQL]
              FROM [##Columns]
              GROUP BY [##Columns].[SchemaName]
                     , [##Columns].[ObjectName] ) AS [K] ;

        EXEC( @SQL ) ;
    END TRY
    BEGIN CATCH
        IF @UseTransaction = 1
            ROLLBACK ;

        UPDATE [a]
        SET
            [a].[ImportSuccess] = 0
          , [a].[ErrorMessage] = ERROR_MESSAGE()
          , [a].[UpdatedDateTime] = GETDATE()
        FROM [ADF].[ARMTemplateForFactory] AS [a]
        WHERE [a].[ARMTemplateForFactoryId] = @ARMTemplateForFactoryId ;

        THROW ;
    END CATCH ;
GO