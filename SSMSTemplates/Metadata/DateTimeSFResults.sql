USE [Velera] ;
GO
RETURN ;

EXEC [dbo].[sp_ImportToTableFromCSV]
    @CSVFile = 'C:\Temp\Untitled 15_2026-01-29-1139.csv'
  , @DatabaseName = 'Velera'
  , @CreateTypedTable = 1
  , @ColumnDelimiter = ','
  , @FieldQuote = '"'
  , @TableName = 'SFGroupedRowCountsTemp'
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

DROP TABLE IF EXISTS [dbo].[SFGroupedRowCounts] ;

SELECT DISTINCT
       [TABLE_CATALOG]
     , [TABLE_SCHEMA]
     , [TABLE_NAME]
     , [COLUMN_NAME]
     , [COLUMN_VALUE]
     , [ROW_COUNT]
INTO [SFGroupedRowCounts]
FROM [dbo].[SFGroupedRowCountsTemp] ;

CREATE UNIQUE CLUSTERED INDEX [x] ON [dbo].[SFGroupedRowCounts]( [TABLE_CATALOG], [TABLE_SCHEMA], [TABLE_NAME], [COLUMN_NAME], [COLUMN_VALUE] ) ;
GO
SELECT
    [TABLE_CATALOG]
  , [TABLE_SCHEMA]
  , [TABLE_NAME]
  , [COLUMN_NAME]
  , [COLUMN_VALUE]
  , [ROW_COUNT]
FROM [dbo].[SFGroupedRowCounts] ;