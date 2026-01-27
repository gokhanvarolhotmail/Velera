USE Velera ;
GO
RETURN ;

EXEC dbo.sp_ImportToTableFromCSV
    @CSVFile = 'C:\Temp\Untitled 13_2026-01-27-1126.csv'
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
  , @CreateClusteredColumnstoreIndex = 1
  , @UseRealInsteadOfNumeric = 0
  , @DateTimeMilliSecondPrecision = 1
  , @IncludeFileRowId = 0 ;

CREATE UNIQUE INDEX ORDINAL_POSITION ON dbo.SnowflakeTables( TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION ) ;

CREATE UNIQUE INDEX COLUMN_NAME ON dbo.SnowflakeTables( TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME ) ;
GO
SELECT
    TABLE_CATALOG
  , TABLE_SCHEMA
  , TABLE_NAME
  , COLUMN_NAME
  , ORDINAL_POSITION
  , COLUMN_DEFAULT
  , IS_NULLABLE
  , DATA_TYPE
  , CHARACTER_MAXIMUM_LENGTH
  , CHARACTER_OCTET_LENGTH
  , NUMERIC_PRECISION
  , NUMERIC_SCALE
  , DATETIME_PRECISION
  , IS_IDENTITY
  , IDENTITY_START
  , IDENTITY_INCREMENT
  , COMMENT
FROM dbo.SnowflakeTables ;
