DECLARE @db NVARCHAR(100) = 'ETF_RESEARCH_DAILY';
DECLARE @infotable NVARCHAR(100) = 'ETFInfo_Basic';
DECLARE @adj_table NVARCHAR(100) = 'ETFCoefAdj';

DECLARE @adjtable_sql NVARCHAR(MAX);
DECLARE @info_table_sql NVARCHAR(MAX);

SET @info_table_sql = '
IF NOT EXISTS (
    SELECT 1 FROM [' + @db + '].sys.objects
    WHERE name = ''' + @infotable + ''' AND type = ''U''
)
BEGIN
    DECLARE @createtable_inner NVARCHAR(MAX) = ''
    CREATE TABLE [' + @db + '].[dbo].[' + @infotable + '] (
        [PK] INT NOT NULL IDENTITY (1, 1),
        [Ticker] VARCHAR(10) NOT NULL,
        [Instrument] VARCHAR(10) NOT NULL,
        [Name] NVARCHAR(MAX) NOT NULL,
        [EstablishDate] DATETIME NULL,
        [ListDate] DATETIME NULL,
        [Exchange] VARCHAR(10) NULL,
        [Style] VARCHAR(MAX) NULL,
        [Target] NVARCHAR(MAX) NULL,
        [TotalIssuedShares] FLOAT NULL,
        [BenchMark] VARCHAR(10) NULL,
        [TradeSymbol] VARCHAR(10) NULL,
        [UpdateTime] DATETIME NULL CONSTRAINT [DF_' + @infotable + '_UpdateTime] DEFAULT (GETDATE()),
        CONSTRAINT [PK_' + @infotable + '] PRIMARY KEY CLUSTERED ([PK] ASC)
            WITH (
                PAD_INDEX = OFF,
                STATISTICS_NORECOMPUTE = OFF,
                IGNORE_DUP_KEY = OFF,
                ALLOW_ROW_LOCKS = ON,
                ALLOW_PAGE_LOCKS = ON
            )
    ) ON [PRIMARY]'';
    EXEC(@createtable_inner);

    DECLARE @createix_inner NVARCHAR(MAX) = ''
    CREATE UNIQUE NONCLUSTERED INDEX ' + @infotable + '_1 ON [' + @db + '].[dbo].[' + @infotable + '] (Ticker)
        WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = ON, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];
    CREATE NONCLUSTERED INDEX ' + @infotable + '_2 ON [' + @db + '].[dbo].[' + @infotable + '] (Ticker, Exchange)
        WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];
    CREATE NONCLUSTERED INDEX ' + @infotable + '_3 ON [' + @db + '].[dbo].[' + @infotable + '] (Ticker, BenchMark)
        WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];
    CREATE NONCLUSTERED INDEX ' + @infotable + '_4 ON [' + @db + '].[dbo].[' + @infotable + '] (Ticker, ListDate)
        WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];
    CREATE NONCLUSTERED INDEX ' + @infotable + '_6 ON [' + @db + '].[dbo].[' + @infotable + '] (Instrument)
        WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];
    '';
    EXEC(@createix_inner);
END
';

EXEC sp_executesql @info_table_sql;


-- 2. 判断是否以存在，若不存在则动态建表与建索引
SET @adjtable_sql = '
IF NOT EXISTS (
    SELECT 1 FROM ' + QUOTENAME(@db) + '.sys.objects
    WHERE name = N''' + @adj_table + ''' AND type = ''U''
)
BEGIN
    EXEC(''
    CREATE TABLE ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@adj_table) + ' (
        [PK] INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
        [TradeDay] DATETIME NOT NULL,
        [Ticker] VARCHAR(10) NOT NULL,
        [CoefAdj] FLOAT NOT NULL,
        [UpdateTime] DATETIME NOT NULL CONSTRAINT DF_' + @adj_table + '_UpdateTime DEFAULT (GETDATE())
    ) ON [PRIMARY]''
    );

    EXEC(''
    CREATE NONCLUSTERED INDEX IX_' + @adj_table + '_TradeDay
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@adj_table) + ' ([TradeDay]);

    CREATE NONCLUSTERED INDEX IX_' + @adj_table + '_Ticker
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@adj_table) + ' ([Ticker]);

    CREATE NONCLUSTERED INDEX IX_' + @adj_table + '_TradeDay_Ticker
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@adj_table) + ' ([TradeDay], [Ticker]);
    '');
END;
';

-- 3. 执行动态 SQL
EXEC sp_executesql @adjtable_sql;