USE [Velera] ;
GO
IF SCHEMA_ID('ADF') IS NULL
    EXEC( 'CREATE SCHEMA ADF' ) ;
GO
IF SCHEMA_ID('ADF_HISTORY') IS NULL
    EXEC( 'CREATE SCHEMA ADF_HISTORY' ) ;
GO
-- SELECT 'DROP TABLE IF EXISTS ADF_HISTORY.' + name + ';' FROM sys.tables WHERE schema_id = SCHEMA_ID('ADF_HISTORY') ;
-- SELECT TOP 100 * FROM ADF.ARMTemplateForFactory ORDER BY ARMTemplateForFactoryId DESC;
-- EXEC ADF.CompareARMTemplateForFactory 6,8
GO
CREATE OR ALTER PROCEDURE [ADF].[CompareARMTemplateForFactory]
    @ARMTemplateForFactoryId_Left  INT
  , @ARMTemplateForFactoryId_Right INT
AS
SET NOCOUNT ON ;

DECLARE @SQL VARCHAR(MAX) ;

IF @ARMTemplateForFactoryId_Left IS NULL OR @ARMTemplateForFactoryId_Right IS NULL OR @ARMTemplateForFactoryId_Left <= 0 OR @ARMTemplateForFactoryId_Right <= 0 OR @ARMTemplateForFactoryId_Left <> @ARMTemplateForFactoryId_Left
    THROW 50000, 'Invalid @ARMTemplateForFactoryId_Left or @ARMTemplateForFactoryId_Left', 1 ;

DROP TABLE IF EXISTS [##Columns] ;

SELECT
    QUOTENAME([s].[name]) + '.' + QUOTENAME([t].[name]) AS [FQN]
  , [s].[name] AS [SchemaName]
  , [t].[name] AS [ObjectName]
  , [c].[name] AS [ColumnName]
  , [c].[column_id] AS [ColumnId]
  , [ic].[key_ordinal] AS [PKOrdinal]
  , [ys].[name] AS [DataType]
  , [c].[collation_name] AS [CollationName]
  , [t].[type] AS [ObjectType]
  , CAST(CASE WHEN [ys].[name] = 'timestamp' THEN 'rowversion'
             WHEN [ys].[name] IN ('char', 'varchar') THEN CONCAT([ys].[name], '(', CASE WHEN [c].[max_length] = -1 THEN 'MAX' ELSE CAST([c].[max_length] AS VARCHAR) END, ')', CASE WHEN [c].[collation_name] <> [d].[collation_name] THEN CONCAT(' COLLATE ', [c].[collation_name]) ELSE '' END)
             WHEN [ys].[name] IN ('nchar', 'nvarchar') THEN CONCAT([ys].[name], '(', CASE WHEN [c].[max_length] = -1 THEN 'MAX' ELSE CAST([c].[max_length] / 2 AS VARCHAR) END, ')', CASE WHEN [c].[collation_name] <> [d].[collation_name] THEN CONCAT(' COLLATE ', [c].[collation_name]) ELSE '' END)
             WHEN [ys].[name] IN ('binary', 'varbinary') THEN CONCAT([ys].[name], '(', CASE WHEN [c].[max_length] = -1 THEN 'MAX' ELSE CAST([c].[max_length] AS VARCHAR) END, ')')
             WHEN [ys].[name] IN ('bigint', 'int', 'smallint', 'tinyint') THEN [ys].[name]
             WHEN [ys].[name] IN ('datetime2', 'time', 'datetimeoffset') THEN CONCAT([ys].[name], '(', [c].[scale], ')')
             WHEN [ys].[name] IN ('numeric', 'decimal') THEN CONCAT([ys].[name], '(', [c].[precision], ', ', [c].[scale], ')')
             ELSE [ys].[name]
         END AS VARCHAR(256)) AS [SystemColumnDef]
  , [c].[is_identity] AS [IsIdentity]
  , [c].[is_nullable] AS [IsNullable]
  , [c].[max_length] AS [MaxLength]
  , [c].[precision] AS [Precision]
  , [c].[scale] AS [Scale]
  , [c].[is_computed] AS [IsComputed]
  , [t].[create_date] AS [ObjectCreateDate]
  , [t].[modify_date] AS [ObjectModifyDate]
  , [t].[object_id] AS [ObjectId]
INTO [##Columns]
FROM [sys].[tables] AS [t]
INNER JOIN [sys].[schemas] AS [s] WITH( NOLOCK )ON [s].[schema_id] = [t].[schema_id]
INNER JOIN [sys].[columns] AS [c] WITH( NOLOCK )ON [c].[object_id] = [t].[object_id]
INNER JOIN [sys].[types] AS [ys] WITH( NOLOCK )ON [ys].[system_type_id] = [c].[system_type_id] AND [ys].[user_type_id] = [ys].[system_type_id]
INNER JOIN [sys].[types] AS [yu] WITH( NOLOCK )ON [yu].[user_type_id] = [c].[user_type_id]
LEFT JOIN [sys].[indexes] AS [i] WITH( NOLOCK )ON [i].[object_id] = [t].[object_id] AND [i].[index_id] = 1
LEFT JOIN [sys].[index_columns] AS [ic] WITH( NOLOCK )ON [ic].[object_id] = [t].[object_id] AND [ic].[index_id] = [i].[index_id] AND [ic].[column_id] = [c].[column_id]
LEFT JOIN [sys].[databases] AS [d] ON [d].[name] = DB_NAME()
WHERE [s].[name] = 'ADF_HISTORY' AND [t].[name] NOT IN ('ARMTemplateForFactory') AND ( [t].[name] LIKE '%[_]' + CAST(@ARMTemplateForFactoryId_Left AS VARCHAR) OR [t].[name] LIKE '%[_]' + CAST(@ARMTemplateForFactoryId_Right AS VARCHAR))
OPTION( RECOMPILE ) ;

WITH [L]
AS ( SELECT
         *
       , REPLACE([a].[ObjectName], '_' + CAST(@ARMTemplateForFactoryId_Left AS VARCHAR), '') AS [MainObject]
     FROM [##Columns] AS [a]
     WHERE [a].[ObjectName] LIKE '%[_]' + CAST(@ARMTemplateForFactoryId_Left AS VARCHAR))
   , [R]
AS ( SELECT
         *
       , REPLACE([a].[ObjectName], '_' + CAST(@ARMTemplateForFactoryId_Right AS VARCHAR), '') AS [MainObject]
     FROM [##Columns] AS [a]
     WHERE [a].[ObjectName] LIKE '%[_]' + CAST(@ARMTemplateForFactoryId_Right AS VARCHAR))
   , [S]
AS ( SELECT
         [L].[MainObject]
       , CONCAT(
             'DROP TABLE IF EXISTS #' , [L].[MainObject], '
SELECT
	'''                                                                    , [L].[MainObject], ''' AS TableName,
	'                                                                                                                , STRING_AGG(CAST('' AS VARCHAR(MAX)) + CASE WHEN [L].[PKOrdinal] IS NOT NULL THEN 'ISNULL(L.' + [L].[ColumnName] + ', R.' + [L].[ColumnName] + ') AS ' + [L].[ColumnName] END, ',
	'                                                                                                                                                                                        ) WITHIN GROUP(ORDER BY [L].[PKOrdinal]
                                                                                                                                                                                                                   , [L].[ColumnName])
           , ',
	CASE WHEN L.__ID IS NULL THEN ''L_MISS'' WHEN R.__ID IS NULL THEN ''R_MISS'' ELSE ''COL_DIFF'' END AS DiffType,
	D.ColumnName,
	D.LeftValue,
	D.RightValue
INTO #'      , [L].[MainObject], '
FROM (SELECT 1 AS __ID, * FROM ADF_HISTORY.', MAX([L].[ObjectName]), ') L
FULL OUTER JOIN (SELECT 1 AS __ID, * FROM ADF_HISTORY.', MAX([R].[ObjectName]), ') R ON '
           , STRING_AGG(CAST('' AS VARCHAR(MAX)) + CASE WHEN [L].[PKOrdinal] IS NOT NULL THEN '(L.' + [L].[ColumnName] + ' = R.' + [L].[ColumnName] + ' OR L.' + [L].[ColumnName] + ' IS NULL AND R.' + [R].[ColumnName] + ' IS NULL)' END, ' AND ') WITHIN GROUP(ORDER BY [L].[PKOrdinal]
                                                                                                                                                                                                                                                                         , [L].[ColumnName])
           , '
CROSS APPLY(SELECT ColumnName, LeftValue, RightValue
FROM (VALUES
	'        , STRING_AGG(CASE WHEN [L].[PKOrdinal] IS NULL THEN CONCAT('(''', [L].[ColumnName], ''', CAST(L.', [L].[ColumnName], ' AS VARCHAR(MAX)), CAST(R.', [L].[ColumnName], ' AS VARCHAR(MAX)))') END, ',
	')        WITHIN GROUP(ORDER BY [L].[PKOrdinal]
                                  , [L].[ColumnName]), ')
D (ColumnName, LeftValue, RightValue)) D
WHERE (D.LeftValue <> D.RightValue OR D.LeftValue IS NULL AND D.RightValue IS NOT NULL OR D.LeftValue IS NOT NULL AND D.RightValue IS NULL)
ORDER BY '   , STRING_AGG(CAST('' AS VARCHAR(MAX)) + CASE WHEN [L].[PKOrdinal] IS NOT NULL THEN 'ISNULL(L.' + [L].[ColumnName] + ', R.' + [L].[ColumnName] + ')' END, ',
	')        WITHIN GROUP(ORDER BY [L].[PKOrdinal]
                                  , [L].[ColumnName]), ', D.ColumnName;

IF @@ROWCOUNT > 0
	SELECT * FROM #', [L].[MainObject], ';
')     AS [SQL]
     FROM [L]
     INNER JOIN [R] ON [L].[MainObject] = [R].[MainObject] AND [L].[ColumnName] = [R].[ColumnName]
     WHERE EXISTS ( SELECT 1 FROM [L] AS [L2] WHERE [L2].[MainObject] = [L].[MainObject] AND [L2].[PKOrdinal] IS NULL )
     GROUP BY [L].[MainObject] )
SELECT @SQL = STRING_AGG(CAST('' AS VARCHAR(MAX)) + [S].[SQL], '

')  WITHIN GROUP(ORDER BY [MainObject])
FROM [S] ;

-- SELECT @SQL AS [@SQL] ;
EXEC( @SQL ) ;
GO