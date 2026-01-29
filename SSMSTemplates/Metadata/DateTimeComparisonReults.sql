USE [Velera] ;
GO
RETURN ;

EXEC [dbo].[sp_ImportToTableFromCSV]
    @CSVFile = 'C:\Temp\date column table counts.csv'
  , @DatabaseName = 'Velera'
  , @CreateTypedTable = 1
  , @ColumnDelimiter = ','
  , @FieldQuote = '"'
  , @TableName = 'SQLGroupedRowCountsTemp'
  , @TypedTableName = NULL
  , @UseVarcharMAX = 0
  , @UseNVarchar4000 = 1
  , @RowCount = NULL
  , @ThrowError = 1
  , @IsLinux = 0
  , @Create_Primary_Key = 0
  , @CreateClusteredColumnstoreIndex = 0
  , @UseRealInsteadOfNumeric = 0
  , @DateTimeMilliSecondPrecision = 1
  , @IncludeFileRowId = 0 ;

DROP TABLE IF EXISTS [SQLGroupedRowCounts] ;

SELECT DISTINCT
       [TABLE_SCHEMA]
     , [TABLE_NAME]
     , [COLUMN_NAME]
     , CAST(NULLIF(NULLIF(TRIM([COLUMN_VALUE]), 'NULL'), '') AS DATETIME2(3)) AS [COLUMN_VALUE]
     , [ROW_COUNT]
INTO [SQLGroupedRowCounts]
FROM [dbo].[SQLGroupedRowCountsTemp] ;

CREATE UNIQUE CLUSTERED INDEX [x] ON [dbo].[SQLGroupedRowCounts]( [TABLE_SCHEMA], [TABLE_NAME], [COLUMN_NAME], [COLUMN_VALUE] ) ;
GO
SELECT
    [t].[TABLE_CATALOG]
  , [t].[TABLE_SCHEMA]
  , [t].[TABLE_NAME]
  , [t].[TABLE_OWNER]
  , [t].[TABLE_TYPE]
  , [t].[IS_TRANSIENT]
  , [t].[ROW_COUNT]
  , [t].[BYTES]
  , [t].[RETENTION_TIME]
  , [t].[CREATED]
  , [t].[LAST_ALTERED]
  , [t].[LAST_DDL]
  , [t].[LAST_DDL_BY]
  , [t].[COMMENT]
  , [t].[IS_TEMPORARY]
  , [t].[IS_ICEBERG]
  , [t].[IS_DYNAMIC]
FROM [dbo].[SnowflakeTables] [t] ;