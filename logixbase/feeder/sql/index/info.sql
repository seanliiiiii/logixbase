DECLARE @db NVARCHAR(100) = 'INDEX_RESEARCH_DAILY';            -- 数据库名
DECLARE @info_table NVARCHAR(100) = 'IndexInfo_Basic';          -- 表名

DECLARE @infotable_sql NVARCHAR(MAX);

SET @infotable_sql = '
IF NOT EXISTS (
    SELECT 1 FROM ' + QUOTENAME(@db) + '.sys.objects
    WHERE name = N''' + @info_table + ''' AND type = ''U''
)
BEGIN
    EXEC(''
    CREATE TABLE ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@info_table) + ' (
        [PK] INT NOT NULL IDENTITY (1, 1),
        [Ticker] VARCHAR(20) NOT NULL,
        [Name] NVARCHAR(MAX) NOT NULL,
        [Exchange] VARCHAR(10) NULL,
        [EstablishDate] DATETIME NULL,
        [ListDate] DATETIME NULL,
        [BeginPoint] FLOAT NULL,
        [SampleSize] INT NULL,
        [IndexClass1] VARCHAR(20) NULL,
        [IndexClass2] VARCHAR(20) NULL,
        [IndexClass3] VARCHAR(20) NULL,
        [UpdateTime] DATETIME NULL CONSTRAINT DF_' + @info_table + '_UpdateTime DEFAULT (GETDATE()),
        CONSTRAINT PK_' + @info_table + ' PRIMARY KEY CLUSTERED ([PK] ASC)
        WITH (
            PAD_INDEX = OFF,
            STATISTICS_NORECOMPUTE = OFF,
            IGNORE_DUP_KEY = OFF,
            ALLOW_ROW_LOCKS = ON,
            ALLOW_PAGE_LOCKS = ON
        )
    ) ON [PRIMARY]''
    );

    EXEC(''
    CREATE UNIQUE NONCLUSTERED INDEX IX_' + @info_table + '_1
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@info_table) + ' ([Ticker])
    WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = ON, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
    ON [PRIMARY];

    CREATE NONCLUSTERED INDEX IX_' + @info_table + '_2
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@info_table) + ' ([Ticker], [IndexClass1])
    WITH (STATISTICS_NORECOMPUTE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
    ON [PRIMARY];

    CREATE NONCLUSTERED INDEX IX_' + @info_table + '_3
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@info_table) + ' ([Ticker], [ListDate])
    WITH (STATISTICS_NORECOMPUTE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
    ON [PRIMARY];

    CREATE NONCLUSTERED INDEX IX_' + @info_table + '_4
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@info_table) + ' ([UpdateTime])
    WITH (STATISTICS_NORECOMPUTE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
    ON [PRIMARY];
    '');
END
';

-- 执行
EXEC sp_executesql @infotable_sql;
