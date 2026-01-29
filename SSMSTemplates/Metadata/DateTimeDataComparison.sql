USE [Velera] ;
GO

-- Original data type
DROP TABLE IF EXISTS [DateTimeDataComparison] ;

SELECT
    [sf].[TABLE_CATALOG] AS [SF_TABLE_CATALOG]
  , [sq].[TABLE_SCHEMA] AS [SQLTABLE_SCHEMA]
  , [sf].[TABLE_SCHEMA] AS [SF_TABLE_SCHEMA]
  , ISNULL([sq].[TABLE_NAME], [sf].[TABLE_NAME]) AS [TABLE_NAME]
  , CASE WHEN [sq].[TABLE_NAME] IS NULL THEN 'SQL' WHEN [sf].[TABLE_NAME] IS NULL THEN 'SF' END AS [MISSING]
  , ISNULL([sq].[COLUMN_NAME], [sf].[COLUMN_NAME]) AS [COLUMN_NAME]
  , ISNULL([sq].[COLUMN_VALUE], [sf].[COLUMN_VALUE]) AS [COLUMN_VALUE]
  , [sq].[ROW_COUNT] AS [SQL_ROW_COUNT]
  , [sf].[ROW_COUNT] AS [SF_ROW_COUNT]
  , ABS(ISNULL([sq].[ROW_COUNT], 0) - ISNULL([sf].[ROW_COUNT], 0)) AS [ABS_ROW_COUNT_DIFF]
INTO [DateTimeDataComparison]
FROM [dbo].[SQLGroupedRowCounts] [sq]
FULL OUTER JOIN [dbo].[SFGroupedRowCounts] [sf] ON [sq].[TABLE_NAME] = [sf].[TABLE_NAME] AND [sq].[COLUMN_NAME] = [sf].[COLUMN_NAME] AND ( [sq].[COLUMN_VALUE] = [sf].[COLUMN_VALUE] OR [sq].[COLUMN_VALUE] IS NULL AND [sf].[COLUMN_VALUE] IS NULL )
WHERE ISNULL([sq].[TABLE_NAME], [sf].[TABLE_NAME])IN( SELECT [TABLE_NAME] FROM [dbo].[Comparison_2] WHERE [Missing] IS NULL )
ORDER BY 4
       , 1
       , 2
       , 3 ;

CREATE UNIQUE CLUSTERED INDEX [CCI] ON [DateTimeDataComparison]( [TABLE_NAME], [SQLTABLE_SCHEMA], [SF_TABLE_SCHEMA], [COLUMN_NAME], [COLUMN_VALUE] ) ;

SELECT
    [SQLTABLE_SCHEMA]
  , [SF_TABLE_SCHEMA]
  , [TABLE_NAME]
  , [MISSING]
  , [COLUMN_NAME]
  , [COLUMN_VALUE]
  , [SQL_ROW_COUNT]
  , [SF_ROW_COUNT]
  , [ABS_ROW_COUNT_DIFF]
FROM [DateTimeDataComparison] ;

-- Date data type
DROP TABLE IF EXISTS [DateDataComparison] ;

SELECT
    [sf].[TABLE_CATALOG] AS [SF_TABLE_CATALOG]
  , [sq].[TABLE_SCHEMA] AS [SQLTABLE_SCHEMA]
  , [sf].[TABLE_SCHEMA] AS [SF_TABLE_SCHEMA]
  , ISNULL([sq].[TABLE_NAME], [sf].[TABLE_NAME]) AS [TABLE_NAME]
  , CASE WHEN [sq].[TABLE_NAME] IS NULL THEN 'SQL' WHEN [sf].[TABLE_NAME] IS NULL THEN 'SF' END AS [MISSING]
  , ISNULL([sq].[COLUMN_NAME], [sf].[COLUMN_NAME]) AS [COLUMN_NAME]
  , ISNULL([sq].[COLUMN_VALUE], [sf].[COLUMN_VALUE]) AS [COLUMN_VALUE]
  , [sq].[ROW_COUNT] AS [SQL_ROW_COUNT]
  , [sf].[ROW_COUNT] AS [SF_ROW_COUNT]
  , ABS(ISNULL([sq].[ROW_COUNT], 0) - ISNULL([sf].[ROW_COUNT], 0)) AS [ABS_ROW_COUNT_DIFF]
INTO [DateDataComparison]
FROM( SELECT
          [TABLE_SCHEMA]
        , [TABLE_NAME]
        , [COLUMN_NAME]
        , CAST([COLUMN_VALUE] AS DATE) AS [COLUMN_VALUE]
        , SUM([ROW_COUNT]) AS [ROW_COUNT]
      FROM [dbo].[SQLGroupedRowCounts]
      GROUP BY [TABLE_SCHEMA]
             , [TABLE_NAME]
             , [COLUMN_NAME]
             , CAST([COLUMN_VALUE] AS DATE)) [sq]
FULL OUTER JOIN( SELECT
                     [TABLE_CATALOG]
                   , [TABLE_SCHEMA]
                   , [TABLE_NAME]
                   , [COLUMN_NAME]
                   , CAST([COLUMN_VALUE] AS DATE) AS [COLUMN_VALUE]
                   , SUM([ROW_COUNT]) AS [ROW_COUNT]
                 FROM [dbo].[SFGroupedRowCounts]
                 GROUP BY [TABLE_CATALOG]
                        , [TABLE_SCHEMA]
                        , [TABLE_NAME]
                        , [COLUMN_NAME]
                        , CAST([COLUMN_VALUE] AS DATE)) [sf] ON [sq].[TABLE_NAME] = [sf].[TABLE_NAME] AND [sq].[COLUMN_NAME] = [sf].[COLUMN_NAME] AND ( [sq].[COLUMN_VALUE] = [sf].[COLUMN_VALUE] OR [sq].[COLUMN_VALUE] IS NULL AND [sf].[COLUMN_VALUE] IS NULL )
WHERE ISNULL([sq].[TABLE_NAME], [sf].[TABLE_NAME])IN( SELECT [TABLE_NAME] FROM [dbo].[Comparison_2] WHERE [Missing] IS NULL )
ORDER BY 4
       , 1
       , 2
       , 3 ;

CREATE UNIQUE CLUSTERED INDEX [CCI] ON [DateDataComparison]( [TABLE_NAME], [SQLTABLE_SCHEMA], [SF_TABLE_SCHEMA], [COLUMN_NAME], [COLUMN_VALUE] ) ;

SELECT
    [SQLTABLE_SCHEMA]
  , [SF_TABLE_SCHEMA]
  , [TABLE_NAME]
  , [MISSING]
  , [COLUMN_NAME]
  , [COLUMN_VALUE]
  , [SQL_ROW_COUNT]
  , [SF_ROW_COUNT]
  , [ABS_ROW_COUNT_DIFF]
FROM [DateDataComparison] ;