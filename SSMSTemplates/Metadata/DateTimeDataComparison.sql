USE [Velera] ;
GO
-- Original data type
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
FROM [dbo].[SQLGroupedRowCounts] [sq]
FULL OUTER JOIN [dbo].[SFGroupedRowCounts] [sf] ON [sq].[TABLE_SCHEMA] = [sf].[TABLE_SCHEMA] AND [sq].[TABLE_NAME] = [sf].[TABLE_NAME] AND [sq].[COLUMN_NAME] = [sf].[COLUMN_NAME] AND ( [sq].[COLUMN_VALUE] = [sf].[COLUMN_VALUE] OR [sq].[COLUMN_VALUE] IS NULL AND [sf].[COLUMN_VALUE] IS NULL )
WHERE ISNULL([sq].[TABLE_NAME], [sf].[TABLE_NAME])IN( SELECT [TABLE_NAME] FROM [dbo].[Comparison_2] WHERE [Missing] IS NULL )
ORDER BY 4
       , 1
       , 2
       , 3 ;



-- Date data type
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
                        , CAST([COLUMN_VALUE] AS DATE)) [sf] ON [sq].[TABLE_SCHEMA] = [sf].[TABLE_SCHEMA]
                                                            AND [sq].[TABLE_NAME] = [sf].[TABLE_NAME]
                                                            AND [sq].[COLUMN_NAME] = [sf].[COLUMN_NAME]
                                                            AND ( [sq].[COLUMN_VALUE] = [sf].[COLUMN_VALUE] OR [sq].[COLUMN_VALUE] IS NULL AND [sf].[COLUMN_VALUE] IS NULL )
WHERE ISNULL([sq].[TABLE_NAME], [sf].[TABLE_NAME])IN( SELECT [TABLE_NAME] FROM [dbo].[Comparison_2] WHERE [Missing] IS NULL )
ORDER BY 4
       , 1
       , 2
       , 3 ;
