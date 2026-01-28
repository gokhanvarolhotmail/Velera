USE [Velera] ;
GO
RETURN ;

SELECT
    [a].[ARMTemplateForFactoryId]
  , [a].[ImportSuccess]
  , [a].[DurationSec]
  , [a].[JSONLength]
  , [a].[JSONCompressedLength]
  , [a].[ErrorMessage]
  , LEN([a].[JSON]) AS [JSON_LENGTH]
  , CHECKSUM([a].[JSON]) AS [JSON_CHECKSUM]
  , [a].[JSON]
  --, [a].[JSONCompressed]
  , [a].[CreatedDateTime]
  , [a].[UpdatedDateTime]
FROM [ADF].[ARMTemplateForFactory] AS [a] ;
GO

SELECT
    [s].[DataflowName]
  , [s].[StepType]
  , [s].[StepOrder]
  , [s].[StepName]
  , [s].[DatasetReference]
  , [s].[DatasetType]
  , [s].[LinkedServiceName]
  , [s].[FolderName]
  , [s].[TargetLocation]
FROM [ADF].[DataFlowSteps] [s] ;

SELECT
    [ca].[PipelineName]
  , [ca].[ActivityName]
  , [ca].*
FROM [ADF].[CopyActivities] [ca] ;

SELECT
    [p].[ParameterName]
  , [p].[DataType]
  , [p].[Metadata]
  , [p].[DefaultValue]
FROM [ADF].[Parameters] AS [p] ;

SELECT
    [g].[ParameterName]
  , [g].[DataType]
  , [g].[Value]
FROM [ADF].[GlobalParameters] AS [g] ;

SELECT
    [i].[IntegrationRuntime]
  , [i].[Type]
  , [i].[DependencyType]
  , [i].[DependencyName]
FROM [ADF].[IntegrationRunTimes] AS [i] ;

SELECT
    [l].[LinkedService]
  , [l].[Type]
  , [l].[ConnectViaReferenceName]
  , [l].[ConnectViaType]
  , [l].[Parameters]
  , [l].[Annotations]
  , [l].[TypeProperties]
  , [l].[Value]
FROM [ADF].[LinkedServices] AS [l] ;

SELECT
    [ds].[Dataset]
  , [ds].[DatasetType]
  , [ds].[LinkedService]
  , [ds].[Folder]
  , [ds].[Description]
  , [ds].[Schema]
  , [ds].[Parameters]
  , [ds].[Annotations]
  , [ds].[TypeProperties]
  , [ds].[TargetTable]
  , [ds].[TargetSchema]
  , [ds].[FileLocation]
  , [ds].[FolderPath]
  , [ds].[FileName]
FROM [ADF].[DataSets] AS [ds] ;

SELECT
    [d].[DataFlow]
  , [d].[Type]
  , [d].[DependencyCnt]
FROM [ADF].[DataFlows] AS [d] ;

SELECT
    [dd].[DataFlow]
  , [dd].[Type]
  , [dd].[DependencyType]
  , [dd].[DependencyName]
FROM [ADF].[DataFlowDependencies] AS [dd] ;

SELECT
    [p].[Pipeline]
  , [p].[PipelineActivityCnt]
  , [p].[ParameterCnt]
  , [p].[ParameterWithValueCnt]
  , [p].[DependsOnCnt]
FROM [ADF].[PipeLines] AS [p] ;

SELECT
    [p].[Pipeline]
  , [p].[ActivitiesCnt]
  , [p].[L1_Activity]
  , [p].[L1_ActivityType]
  , [p].[L1_Id]
  , [p].[L1_ChildrenCnt]
  --, [p].[L1_SwitchExpressionValue]
  --, [p].[L1_SwitchExpressionType]
  --, [p].[L1_IFExpressionValue]
  --, [p].[L1_IFExpressionType]
  , [p].[L2_Activity]
  , [p].[L2_ActivityType]
  , [p].[L2_Id]
  , [p].[L2_ChildrenCnt]
  , [p].[L2_SwitchValue]
  , [p].[L2_IFCondition]
  --, [p].[L2_SwitchExpressionValue]
  --, [p].[L2_SwitchExpressionType]
  --, [p].[L2_IFExpressionValue]
  --, [p].[L2_IFExpressionType]
  , [p].[L3_Id]
  , [p].[L3_Activity]
  , [p].[L3_ActivityType]
  , [p].[L3_SwitchValue]
  , [p].[L3_IFCondition]
FROM [ADF].[PipeLineActivitiesPivot] AS [p] ;

SELECT
    [p].[Pipeline]
  , [p].[PipelineActivitiesCnt]
  , [p].[Level]
  , [p].[ChildrenCnt]
  , [p].[ParentId]
  , [p].[Id]
  , [p].[Activity]
  , [p].[ActivityType]
  , [p].[ParentActivity]
  , [p].[ParentActivityType]
  , [p].[SwitchExpressionValue]
  , [p].[SwitchExpressionType]
  , [p].[IFExpressionValue]
  , [p].[IFExpressionType]
  , [p].[SwitchValue]
  , [p].[IFCondition]
FROM [ADF].[PipeLineActivities] AS [p] ;

SELECT
    [p].[Pipeline]
  , [p].[ActivityName]
  , [p].[ActivityType]
  , [p].[Level]
  , [p].[ActivityId]
  , [p].[ActivityValue]
FROM [ADF].[PipeLineAllActivities] AS [p] ;

SELECT
    [f].[Pipeline]
  , [f].[Id]
  , [f].[Activity]
  , [f].[ActivityType]
  , [f].[ForEachItems]
  , [f].[ForEachType]
  , [f].[ForEachBatchCount]
FROM [ADF].[ForEach] AS [f] ;

SELECT
    [t].[Pipeline]
  , [t].[Activity]
  , [t].[ActivityType]
  , [t].[QueryId]
  , [t].[Query]
  , [t].[Type]
FROM [ADF].[SQLQueries] AS [t] ;

SELECT
    [q].[Pipeline]
  , [q].[Activity]
  , [q].[ActivityType]
  , [q].[ActivityId]
  , [q].[Query]
FROM [ADF].[ActivityQueries] AS [q] ;

SELECT
    [v].[Pipeline]
  , [v].[Activity]
  , [v].[ActivityType]
  , [v].[ActivityId]
  , [v].[VariableName]
  , [v].[Query]
FROM [ADF].[VariableAssigments] AS [v] ;

SELECT
    [t].[Pipeline]
  , [t].[Activity]
  , [t].[ActivityType]
  , [t].[TimeOut]
  , [t].[Retry]
  , [t].[RetryIntervalInSeconds]
  , [t].[SecureOutput]
  , [t].[SecureInput]
  , [t].[DataIntegrationUnits]
  , [t].[EnableStaging]
FROM [ADF].[ActivityProperties] AS [t] ;

SELECT
    [pp].[Pipeline]
  , [pp].[ParameterCnt]
  , [pp].[ParameterWithValueCnt]
  , [pp].[ParameterName]
  , [pp].[ParameterType]
  , [pp].[ParameterDefaultValue]
FROM [ADF].[PipeLineParameters] AS [pp] ;

SELECT
    [pd].[Pipeline]
  , [pd].[DependsOnType]
  , [pd].[DependsOnName]
FROM [ADF].[PipeLineDependencies] AS [pd] ;

SELECT
    [pdg].[Pipeline]
  , [pdg].[DependsOnType]
  , [pdg].[DependsOnCnt]
  , [pdg].[DependsOnList]
FROM [ADF].[PipeLineDependenciesGrouped] AS [pdg] ;

SELECT
    [t].[TriggerName]
  , [t].[RunTimeState]
  , [t].[PipeLineCnt]
  , [t].[PipeLines]
  , [t].[TriggerType]
  , [t].[Frequency]
  , [t].[Interval]
  , [t].[StartTime]
  , [t].[TimeZone]
FROM [ADF].[Triggers] AS [t] ;

SELECT
    [tt].[TriggerName]
  , [tt].[RunTimeState]
  , [tt].[ScheduleTimeCnt]
  , [tt].[ScheduleTimeId]
  , [tt].[TriggerType]
  , [tt].[Frequency]
  , [tt].[Interval]
  , [tt].[StartTime]
  , [tt].[TimeZone]
  , [tt].[ScheduleTime]
FROM [ADF].[TriggerTimes] AS [tt] ;

SELECT
    [tp].[TriggerName]
  , [tp].[RunTimeState]
  , [tp].[ParameterCnt]
  , [tp].[ParameterName]
  , [tp].[ParameterType]
  , [tp].[ParameterDefaultValue]
FROM [ADF].[TriggerParameters] AS [tp] ;

SELECT
    [tpl].[TriggerName]
  , [tpl].[RunTimeState]
  , [tpl].[PipeLineCnt]
  , [tpl].[PipeLine]
FROM [ADF].[TriggerPipeLines] AS [tpl] ;

SELECT
    [t].[Pipeline]
  , [t].[StoredProcedureName]
  , [t].[StoredProcedureId]
  , [t].[ActivityTypeList]
  , [t].[StoredProcedureCnt]
  , [t].[StoredProcedureParamCnt]
  , [t].[StoredProcedureParamNotNullCnt]
FROM [ADF].[StoredProcedureOrLookup] AS [t] ;

SELECT
    [t].[Pipeline]
  , [t].[Activity]
  , [t].[ActivityType]
  , [t].[StoredProcedureName]
  , [t].[StoredProcedureId]
  , [t].[StoredProcedureParamId]
  , [t].[ParameterName]
  , [t].[ParameterDataType]
  , [t].[ParameterType]
  , [t].[ParameterValue]
FROM [ADF].[StoredProcedureOrLookupParameters] AS [t] ;

SELECT
    [a].[Pipeline]
  , [a].[ActivityName]
  , [a].[Level]
  , [a].[ActivityType]
  , [a].[InputOrOutput]
  , [a].[InputOutputId]
  , [a].[ReferenceName]
  , [a].[ReferenceType]
  , [a].[LinkedServiceReference]
  , [a].[ReferenceValue]
FROM [ADF].[ActivityReferences] AS [a]
ORDER BY [a].[Pipeline]
       , [a].[ActivityName]
       , [a].[InputOrOutput]
       , [a].[InputOutputId] ;
