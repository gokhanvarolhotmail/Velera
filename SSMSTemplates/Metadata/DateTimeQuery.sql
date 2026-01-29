USE [Velera] ;
GO
SELECT
    [t].[schema_name]
  , [t].[object_name] AS [table_name]
  , [t].[type_desc]
  , [t].[rows]
  , [t].[create_date]
  , [t].[modify_date]
  , [c].[DateColumnsCnt]
  , [c].[DateColumns]
FROM [dbo].[SQLTables] [t]
LEFT JOIN( SELECT
               [TABLE_SCHEMA]
             , [TABLE_NAME]
             , COUNT(1) AS [DateColumnsCnt]
             , STRING_AGG(CAST(CONCAT([COLUMN_NAME], ' ', [DATA_TYPE]) AS VARCHAR(MAX)), ', ') WITHIN GROUP(ORDER BY [COLUMN_NAME]) AS [DateColumns]
           FROM [dbo].[SQLColumns]
           WHERE [DATA_TYPE] IN ('date', 'datetime', 'datetime2')
           GROUP BY [TABLE_SCHEMA]
                  , [TABLE_NAME] ) [c] ON [t].[schema_name] = [c].[TABLE_SCHEMA] AND [t].[object_name] = [c].[TABLE_NAME]
WHERE [t].[rows] IS NOT NULL AND EXISTS ( SELECT * FROM [Comparison_2] [r] WHERE [r].[Missing] IS NULL AND [r].[sql_table_schema] = [t].[schema_name] AND [r].[table_name] = [t].[object_name] )
ORDER BY [t].[schema_name]
       , [t].[object_name] ;
