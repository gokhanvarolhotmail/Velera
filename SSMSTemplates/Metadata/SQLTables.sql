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
  , @IsLinux = 0
  , @Create_Primary_Key = 0
  , @CreateClusteredColumnstoreIndex = 0
  , @UseRealInsteadOfNumeric = 0
  , @DateTimeMilliSecondPrecision = 1
  , @IncludeFileRowId = 0 ;

EXEC [dbo].[sp_ImportToTableFromCSV]
    @CSVFile = 'c:\temp\table counts.txt'
  , @DatabaseName = 'Velera'
  , @CreateTypedTable = 1
  , @ColumnDelimiter = ','
  , @FieldQuote = '"'
  , @TableName = 'SQLTablesCounts'
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

DROP TABLE IF EXISTS [dbo].[SQLTables] ;

SELECT
    [t].[schema_name]
  , [t].[name] AS [object_name]
  , TRIM([t].[type]) AS [type]
  , TRIM([t].[type_desc]) AS [type_desc]
  , [r].[row_count] AS [rows]
  , [t].[create_date]
  , [t].[modify_date]
INTO [SQLTables]
FROM [SQLTablesTemp] [t]
LEFT JOIN [SQLTablesCounts] [r] ON [r].[schema_name] = [t].[schema_name] AND [r].[table_name] = [t].[name]
WHERE TRIM([t].[type]) IN ('U', 'V') ;

DROP TABLE IF EXISTS [SQLTablesTemp] ;
DROP TABLE IF EXISTS [SQLTablesCounts] ;

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