USE [Velera] ;
GO
RETURN ;

EXEC [dbo].[sp_ImportToTableFromCSV]
    @CSVFile = 'c:\temp\inform schema.csv'
  , @DatabaseName = 'Velera'
  , @CreateTypedTable = 1
  , @ColumnDelimiter = ','
  , @FieldQuote = '"'
  , @TableName = 'SQLColumns'
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

CREATE UNIQUE CLUSTERED INDEX [COLUMN_NAME] ON [dbo].[SQLColumns]( [TABLE_CATALOG], [TABLE_SCHEMA], [TABLE_NAME], [COLUMN_NAME] ) ;

CREATE UNIQUE INDEX [ORDINAL_POSITION] ON [dbo].[SQLColumns]( [TABLE_CATALOG], [TABLE_SCHEMA], [TABLE_NAME], [ORDINAL_POSITION] ) ;
GO
SELECT
    [c].[TABLE_CATALOG]
  , [c].[TABLE_SCHEMA]
  , [c].[TABLE_NAME]
  , [c].[COLUMN_NAME]
  , [c].[ORDINAL_POSITION]
  , [c].[COLUMN_DEFAULT]
  , [c].[IS_NULLABLE]
  , [c].[DATA_TYPE]
  , [c].[CHARACTER_MAXIMUM_LENGTH]
  , [c].[CHARACTER_OCTET_LENGTH]
  , [c].[NUMERIC_PRECISION]
  , [c].[NUMERIC_PRECISION_RADIX]
  , [c].[NUMERIC_SCALE]
  , [c].[DATETIME_PRECISION]
  , [c].[CHARACTER_SET_CATALOG]
  , [c].[CHARACTER_SET_SCHEMA]
  , [c].[CHARACTER_SET_NAME]
  , [c].[COLLATION_CATALOG]
  , [c].[COLLATION_SCHEMA]
  , [c].[COLLATION_NAME]
  , [c].[DOMAIN_CATALOG]
  , [c].[DOMAIN_SCHEMA]
  , [c].[DOMAIN_NAME]
FROM [dbo].[SQLColumns] [c] ;