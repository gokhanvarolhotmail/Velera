USE [Velera] ;
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
    [pd].[Pipeline]
  , [pd].[DependsOnType]
  , [pd].[DependsOnName]
FROM [ADF].[PipeLineDependencies] AS [pd] ;

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
  , [a].[LinkedService]
  , [a].[ReferenceValue]
FROM [ADF].[ActivityReferences] AS [a]
ORDER BY [a].[Pipeline]
       , [a].[ActivityName]
       , [a].[InputOrOutput]
       , [a].[InputOutputId] ;
