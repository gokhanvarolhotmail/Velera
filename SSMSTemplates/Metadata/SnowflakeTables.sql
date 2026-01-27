USE [Velera] ;
GO
RETURN ;

EXEC [dbo].[sp_ImportToTableFromCSV]
    @CSVFile = 'C:\Temp\Untitled 13_2026-01-27-1124.csv'
  , @DatabaseName = 'Velera'
  , @CreateTypedTable = 1
  , @ColumnDelimiter = ','
  , @FieldQuote = '"'
  , @TableName = 'SnowflakeTables'
  , @TypedTableName = NULL
  , @UseVarcharMAX = 0
  , @UseNVarchar4000 = 1
  , @RowCount = NULL
  , @ThrowError = 1
  , @IsLinux = 1
  , @Create_Primary_Key = 0
  , @CreateClusteredColumnstoreIndex = 0
  , @UseRealInsteadOfNumeric = 0
  , @DateTimeMilliSecondPrecision = 1
  , @IncludeFileRowId = 0 ;

CREATE UNIQUE CLUSTERED INDEX [COLUMN_NAME] ON [dbo].[SnowflakeTables]( [TABLE_CATALOG], [TABLE_SCHEMA], [TABLE_NAME] ) ;
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