USE [Velera] ;
GO
RETURN ;

EXEC [dbo].[sp_ImportToTableFromCSV]
    @CSVFile = 'c:\temp\TABLE AND COUNTS.csv'
  , @DatabaseName = 'Velera'
  , @CreateTypedTable = 1
  , @ColumnDelimiter = ','
  , @FieldQuote = '"'
  , @TableName = 'SQLTablesTemp'
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

DROP TABLE IF EXISTS [dbo].[SQLTables] ;

SELECT
    [schema_name]
  , [name] AS [object_name]
  , TRIM([type]) AS [type]
  , TRIM([type_desc]) AS [type_desc]
  , [rows]
  , [create_date]
  , [modify_date]
INTO [SQLTables]
FROM [SQLTablesTemp] ;

DROP TABLE IF EXISTS [SQLTablesTemp] ;

CREATE UNIQUE CLUSTERED INDEX [name] ON [dbo].[SQLTables]( [schema_name], [object_name] ) ;
GO

SELECT
    [s].[schema_name]
  , [s].[object_name]
  , [s].[type]
  , [s].[type_desc]
  , [s].[rows]
  , [s].[create_date]
  , [s].[modify_date]
FROM [dbo].[SQLTables] [s] ;