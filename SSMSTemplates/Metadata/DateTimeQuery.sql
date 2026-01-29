USE [Velera] ;
GO
SELECT
    [t].[schema_name]
  , [t].[object_name] AS [table_name]
  , [t].[type_desc]
  , [t].[rows] AS [SQL_ROW_COUNT]
  , [r].[SF_ROW_COUNT]
  , [t].[create_date]
  , [t].[modify_date]
  , [c].[DateColumnsCnt]
  , [c].[ColumnsCnt]
  , [c].[DateColumns]
  , [c].[Columns]
FROM [dbo].[SQLTables] [t]
LEFT JOIN( SELECT
               [TABLE_SCHEMA]
             , [TABLE_NAME]
             , SUM(CASE WHEN [DATA_TYPE] IN ('date', 'datetime', 'datetime2') THEN 1 ELSE 0 END) AS [DateColumnsCnt]
             , COUNT(1) AS [ColumnsCnt]
             , STRING_AGG(CASE WHEN [DATA_TYPE] IN ('date', 'datetime', 'datetime2') THEN CAST(CONCAT([COLUMN_NAME], ' ', [DATA_TYPE]) AS VARCHAR(MAX)) END, ', ') WITHIN GROUP(ORDER BY [COLUMN_NAME]) AS [DateColumns]
             , STRING_AGG(CAST(CONCAT([COLUMN_NAME], ' ', [DATA_TYPE]) AS VARCHAR(MAX)), ', ') WITHIN GROUP(ORDER BY [COLUMN_NAME]) AS [Columns]
           FROM [dbo].[SQLColumns]
           GROUP BY [TABLE_SCHEMA]
                  , [TABLE_NAME] ) [c] ON [t].[schema_name] = [c].[TABLE_SCHEMA] AND [t].[object_name] = [c].[TABLE_NAME]
LEFT JOIN [Comparison_2] [r] ON [r].[Missing] IS NULL AND [r].[sql_table_schema] = [t].[schema_name] AND [r].[TABLE_NAME] = [t].[object_name]
WHERE [t].[rows] IS NOT NULL AND [r].[sql_table_schema] IS NOT NULL
ORDER BY [t].[schema_name]
       , [t].[object_name] ;
