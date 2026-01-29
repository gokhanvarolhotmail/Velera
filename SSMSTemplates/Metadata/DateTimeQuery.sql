USE [Velera] ;
GO
DROP TABLE IF EXISTS [#CommonColumns] ;

SELECT
    [c].[TABLE_SCHEMA]
  , [c].[TABLE_NAME]
  , [c].[COLUMN_NAME]
  , [c].[DATA_TYPE]
  , MAX([c].[ORDINAL_POSITION]) AS [max_ordinal_position]
INTO [#CommonColumns]
FROM [dbo].[SnowflakeColumns] [s]
INNER JOIN [dbo].[SQLColumns] [c] ON [s].[TABLE_NAME] = [c].[TABLE_NAME] AND [s].[COLUMN_NAME] = [c].[COLUMN_NAME]
WHERE [s].[DATA_TYPE] IN ('TIMESTAMP_LTZ', 'DATE', 'TIMESTAMP_NTZ') AND [c].[DATA_TYPE] IN ('date', 'datetime', 'datetime2')
GROUP BY [c].[TABLE_SCHEMA]
       , [c].[TABLE_NAME]
       , [c].[COLUMN_NAME]
       , [c].[DATA_TYPE] ;

DROP TABLE IF EXISTS [#ColumnMatches] ;

SELECT
    [t].[schema_name] AS [SQL_TABLE_SCHEMA]
  , [r].[SF_TABLE_CATALOG]
  , [r].[SF_TABLE_SCHEMA]
  , [t].[object_name] AS [TABLE_NAME]
  , [t].[type_desc]
  , [t].[rows] AS [SQL_ROW_COUNT]
  , [r].[SF_ROW_COUNT]
  , [t].[create_date]
  , [t].[modify_date]
  , [mc].[MatchedDateColumn]
  , [mc].[CommonDateColumnCnt]
  , [c].[DateColumnsCnt]
  , [c].[AllColumnsCnt]
  , [mc].[CommonDateColumns]
  , [c].[DateColumns]
  , [c].[AllColumns]
INTO [#ColumnMatches]
FROM [dbo].[SQLTables] [t]
LEFT JOIN( SELECT
               [c].[TABLE_SCHEMA]
             , [c].[TABLE_NAME]
             , [c].[CommonDateColumnCnt]
             , [c].[CommonDateColumns]
             , ( SELECT TOP 1
                        [cc].[COLUMN_NAME]
                 FROM [#CommonColumns] [cc]
                 WHERE [cc].[TABLE_SCHEMA] = [c].[TABLE_SCHEMA] AND [cc].[TABLE_NAME] = [c].[TABLE_NAME]
                 ORDER BY CASE WHEN [cc].[COLUMN_NAME] LIKE 'load%date%' THEN 1 WHEN [cc].[COLUMN_NAME] LIKE 'modified%date' THEN 2 WHEN [cc].[COLUMN_NAME] LIKE 'create%date%' THEN 3 ELSE 4 END
                        , [cc].[max_ordinal_position] DESC ) AS [MatchedDateColumn]
           FROM( SELECT
                     [c].[TABLE_SCHEMA]
                   , [c].[TABLE_NAME]
                   , COUNT(1) AS [CommonDateColumnCnt]
                   , STRING_AGG(CAST(CONCAT([c].[COLUMN_NAME], ' ', [c].[DATA_TYPE]) AS VARCHAR(MAX)), ', ') WITHIN GROUP(ORDER BY [c].[COLUMN_NAME]) AS [CommonDateColumns]
                 FROM [#CommonColumns] [c]
                 GROUP BY [c].[TABLE_SCHEMA]
                        , [c].[TABLE_NAME] ) [c] ) [mc] ON [mc].[TABLE_SCHEMA] = [t].[schema_name] AND [mc].[TABLE_NAME] = [t].[object_name]
LEFT JOIN( SELECT
               [TABLE_SCHEMA]
             , [TABLE_NAME]
             , SUM(CASE WHEN [DATA_TYPE] IN ('date', 'datetime', 'datetime2') THEN 1 ELSE 0 END) AS [DateColumnsCnt]
             , COUNT(1) AS [AllColumnsCnt]
             , STRING_AGG(CASE WHEN [DATA_TYPE] IN ('date', 'datetime', 'datetime2') THEN CAST(CONCAT([COLUMN_NAME], ' ', [DATA_TYPE]) AS VARCHAR(MAX)) END, ', ') WITHIN GROUP(ORDER BY [COLUMN_NAME]) AS [DateColumns]
             , STRING_AGG(CAST(CONCAT([COLUMN_NAME], ' ', [DATA_TYPE]) AS VARCHAR(MAX)), ', ') WITHIN GROUP(ORDER BY [COLUMN_NAME]) AS [AllColumns]
           FROM [dbo].[SQLColumns]
           GROUP BY [TABLE_SCHEMA]
                  , [TABLE_NAME] ) [c] ON [t].[schema_name] = [c].[TABLE_SCHEMA] AND [t].[object_name] = [c].[TABLE_NAME]
LEFT JOIN [dbo].[Comparison_2] [r] ON [r].[Missing] IS NULL AND [r].[SQL_TABLE_SCHEMA] = [t].[schema_name] AND [r].[TABLE_NAME] = [t].[object_name]
WHERE [t].[rows] IS NOT NULL AND [r].[SQL_TABLE_SCHEMA] IS NOT NULL
ORDER BY [t].[schema_name]
       , [t].[object_name] ;

CREATE UNIQUE CLUSTERED INDEX [CCI] ON [#ColumnMatches]( [SQL_TABLE_SCHEMA], [SF_TABLE_SCHEMA], [TABLE_NAME] ) ;

-- SQL POOL
SELECT 'CREATE TABLE #TEMP([TABLE_SCHEMA] VARCHAR(128), [TABLE_NAME] VARCHAR(128), [COLUMN_NAME] VARCHAR(128), [COLUMN_VALUE] DATETIME2(3), [ROW_COUNT] BIGINT);

'   AS [SQL]
UNION ALL
SELECT CONCAT('INSERT INTO #TEMP
SELECT ''', [A].[SQL_TABLE_SCHEMA], ''' AS [TABLE_SCHEMA], ''', [A].[TABLE_NAME], ''' AS [TABLE_NAME], ''', [A].[MatchedDateColumn], ''' AS [COLUMN_NAME], ', QUOTENAME([A].[MatchedDateColumn]), ' AS [COLUMN_VALUE], COUNT_BIG(1) AS [ROW_COUNT]
FROM ', QUOTENAME([A].[SQL_TABLE_SCHEMA]), '.', QUOTENAME([A].[TABLE_NAME]), '
GROUP BY ', QUOTENAME([A].[MatchedDateColumn]), ';

')  AS [SQL]
FROM( SELECT TOP 999999 * FROM [#ColumnMatches] WHERE [MatchedDateColumn] IS NOT NULL ORDER BY [SQL_TABLE_SCHEMA], [TABLE_NAME] ) [A]
UNION ALL
SELECT '
SELECT [TABLE_SCHEMA], [TABLE_NAME], [COLUMN_NAME], [COLUMN_VALUE], [ROW_COUNT]
FROM #TEMP
ORDER BY [TABLE_SCHEMA], [TABLE_NAME];' AS [SQL] ;

-- SNOWFLAKE
SELECT 'CREATE OR REPLACE TEMPORARY TABLE TEMP(TABLE_SCHEMA VARCHAR(128), TABLE_NAME VARCHAR(128), COLUMN_NAME VARCHAR(128), COLUMN_VALUE TIMESTAMP_NTZ, ROW_COUNT BIGINT);

'   AS [SQL]
UNION ALL
SELECT CONCAT('INSERT INTO TEMP
SELECT ''', [A].[SF_TABLE_SCHEMA], ''' AS TABLE_SCHEMA, ''', [A].[TABLE_NAME], ''' AS [TABLE_NAME], ''', [A].[MatchedDateColumn], ''' AS COLUMN_NAME, ', QUOTENAME([A].[MatchedDateColumn], '"'), ' AS COLUMN_VALUE, COUNT(1) AS ROW_COUNT
FROM ', QUOTENAME([A].[SF_TABLE_CATALOG], '"'), '.', QUOTENAME([A].[SF_TABLE_SCHEMA], '"'), '.', QUOTENAME([A].[TABLE_NAME], '"'), '
GROUP BY ', QUOTENAME([A].[MatchedDateColumn], '"'), ';

')  AS [SQL]
FROM( SELECT TOP 999999 * FROM [#ColumnMatches] WHERE [MatchedDateColumn] IS NOT NULL ORDER BY [SQL_TABLE_SCHEMA], [TABLE_NAME] ) [A]
UNION ALL
SELECT '
SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, COLUMN_VALUE, ROW_COUNT
FROM TEMP
ORDER BY TABLE_SCHEMA, TABLE_NAME;' AS [SQL] ;
