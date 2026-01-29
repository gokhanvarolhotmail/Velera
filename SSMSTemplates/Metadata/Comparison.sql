USE [Velera] ;
GO
DROP TABLE IF EXISTS [Comparison_1] ;
DROP TABLE IF EXISTS [#Snowflake] ;
DROP TABLE IF EXISTS [#SQL] ;

SELECT
    [t].[TABLE_CATALOG]
  , [t].[TABLE_SCHEMA]
  , [t].[TABLE_NAME]
  , [t].[TABLE_TYPE]
  , [t].[ROW_COUNT]
  , [t].[CREATED]
  , [t].[LAST_ALTERED]
  , [c].[ColumnCnt]
  , [c].[ColumnList]
  , [t].[BYTES]
  , CAST([t].[BYTES] / 1024.0 / 1024.0 AS NUMERIC(20, 3)) AS [MBYTES]
  , [t].[RETENTION_TIME]
  , [t].[TABLE_OWNER]
  , [t].[IS_TRANSIENT]
  , [t].[LAST_DDL]
  , [t].[LAST_DDL_BY]
  , [t].[COMMENT]
  , [t].[IS_TEMPORARY]
  , [t].[IS_ICEBERG]
  , [t].[IS_DYNAMIC]
INTO [#Snowflake]
FROM [dbo].[SnowflakeTables] [t]
LEFT JOIN( SELECT
               [TABLE_CATALOG]
             , [TABLE_SCHEMA]
             , [TABLE_NAME]
             , COUNT(1) AS [ColumnCnt]
             , STRING_AGG(CAST([COLUMN_NAME] AS VARCHAR(MAX)), ', ') WITHIN GROUP(ORDER BY [COLUMN_NAME]) AS [ColumnList]
           FROM [dbo].[SnowflakeColumns]
           GROUP BY [TABLE_CATALOG]
                  , [TABLE_SCHEMA]
                  , [TABLE_NAME] ) [c] ON [t].[TABLE_CATALOG] = [c].[TABLE_CATALOG] AND [t].[TABLE_SCHEMA] = [c].[TABLE_SCHEMA] AND [t].[TABLE_NAME] = [c].[TABLE_NAME] ;

CREATE UNIQUE CLUSTERED INDEX [table_name] ON [#Snowflake]( [TABLE_CATALOG], [TABLE_SCHEMA], [TABLE_NAME] ) ;

SELECT
    'ConsolidatedDW' AS [TABLE_CATALOG]
  , [t].[schema_name] AS [TABLE_SCHEMA]
  , [t].[object_name] AS [TABLE_NAME]
  , [t].[type_desc] AS [TABLE_TYPE]
  , [t].[rows] AS [ROW_COUNT]
  , [t].[create_date] AS [CREATED]
  , [t].[modify_date] AS [LAST_ALTERED]
  , [c].[ColumnCnt]
  , [c].[ColumnList]
INTO [#SQL]
FROM [dbo].[SQLTables] [t]
LEFT JOIN( SELECT
               [TABLE_CATALOG]
             , [TABLE_SCHEMA]
             , [TABLE_NAME]
             , COUNT(1) AS [ColumnCnt]
             , STRING_AGG(CAST([COLUMN_NAME] AS VARCHAR(MAX)), ', ') WITHIN GROUP(ORDER BY [COLUMN_NAME]) AS [ColumnList]
           FROM [dbo].[SQLColumns]
           GROUP BY [TABLE_CATALOG]
                  , [TABLE_SCHEMA]
                  , [TABLE_NAME] ) [c] ON [t].[schema_name] = [c].[TABLE_SCHEMA] AND [t].[object_name] = [c].[TABLE_NAME] ;

CREATE UNIQUE CLUSTERED INDEX [table_name] ON [#SQL]( [TABLE_CATALOG], [TABLE_SCHEMA], [TABLE_NAME] ) ;

SELECT
    ISNULL([a].[TABLE_CATALOG], [b].[TABLE_CATALOG]) AS [TABLE_CATALOG]
  , ISNULL([a].[TABLE_SCHEMA], [b].[TABLE_SCHEMA]) AS [TABLE_SCHEMA]
  , ISNULL([a].[TABLE_NAME], [b].[TABLE_NAME]) AS [TABLE_NAME]
  , CASE WHEN [a].[TABLE_SCHEMA] IS NULL THEN 'Snowflake' WHEN [b].[TABLE_SCHEMA] IS NULL THEN 'SQL' END AS [Missing]
  , [a].[TABLE_TYPE] AS [SF_TABLE_TYPE]
  , [b].[TABLE_TYPE] AS [SQL_TABLE_TYPE]
  , [a].[ROW_COUNT] AS [SF_ROW_COUNT]
  , [b].[ROW_COUNT] AS [SQL_ROW_COUNT]
  , [a].[ColumnCnt] AS [SF_ColumnCnt]
  , [b].[ColumnCnt] AS [SQL_ColumnCnt]
  , [a].[ColumnList] AS [SF_ColumnList]
  , [b].[ColumnList] AS [SQL_ColumnList]
  , [a].[CREATED] AS [SF_CREATED]
  , [b].[CREATED] AS [SQL_CREATED]
  , [a].[LAST_ALTERED] AS [SF_LAST_ALTERED]
  , [b].[LAST_ALTERED] AS [SQL_LAST_ALTERED]
INTO [Comparison_1]
FROM [#Snowflake] [a]
LEFT JOIN [#SQL] [b] ON [a].[TABLE_SCHEMA] = [b].[TABLE_SCHEMA] AND [a].[TABLE_NAME] = [b].[TABLE_NAME] ;

CREATE UNIQUE CLUSTERED INDEX [table_name] ON [Comparison_1]( [TABLE_CATALOG], [TABLE_SCHEMA], [TABLE_NAME] ) ;

SELECT
    [c].[TABLE_CATALOG]
  , [c].[TABLE_SCHEMA]
  , [c].[TABLE_NAME]
  , [c].[Missing]
  , [c].[SF_TABLE_TYPE]
  , [c].[SQL_TABLE_TYPE]
  , [c].[SF_ROW_COUNT]
  , [c].[SQL_ROW_COUNT]
  , [c].[SF_ColumnCnt]
  , [c].[SQL_ColumnCnt]
  , [c].[SF_ColumnList]
  , [c].[SQL_ColumnList]
  , [c].[SF_CREATED]
  , [c].[SQL_CREATED]
  , [c].[SF_LAST_ALTERED]
  , [c].[SQL_LAST_ALTERED]
FROM [Comparison_1] [c] ;

RETURN ;

SELECT *
FROM [#Snowflake] ;

SELECT *
FROM [#SQL] ;