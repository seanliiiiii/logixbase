DECLARE @db NVARCHAR(100) = 'STOCK_RESEARCH_DAILY';
DECLARE @info_table NVARCHAR(100) = 'StockInfo_Basic';
DECLARE @adj_table NVARCHAR(100) = 'StockCoefAdj';

DECLARE @adjtable_sql NVARCHAR(MAX);
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
        [Ticker] NVARCHAR(10) NOT NULL,
        [Instrument] NVARCHAR(10) NOT NULL,
        [Name] NVARCHAR(10) NOT NULL,
        [RegisteredCapital] FLOAT NULL,
        [Representative] NVARCHAR(MAX) NULL,
        [EstablishDate] DATETIME NULL,
        [ListDate] DATETIME NULL,
        [Exchange] VARCHAR(10) NULL,
        [Board] VARCHAR(10) NULL,
        [TotalShares] FLOAT NULL,
        [TotalTradeableShares] FLOAT NULL,
        [MainBusiness] NVARCHAR(MAX) NULL,
        [Status] VARCHAR(10) NULL,
        [SWClassLevel1] VARCHAR(20) NULL,
        [SWClassLevel2] VARCHAR(20) NULL,
        [SWClassLevel3] VARCHAR(20) NULL,
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
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@info_table) + ' (Ticker)
    WITH (
        STATISTICS_NORECOMPUTE = OFF,
        IGNORE_DUP_KEY = ON,
        ALLOW_ROW_LOCKS = ON,
        ALLOW_PAGE_LOCKS = ON
    ) ON [PRIMARY]''
    );

    EXEC(''
    CREATE NONCLUSTERED INDEX IX_' + @info_table + '_2
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@info_table) + ' (Ticker, Board)
    WITH (
        STATISTICS_NORECOMPUTE = OFF,
        IGNORE_DUP_KEY = OFF,
        ALLOW_ROW_LOCKS = ON,
        ALLOW_PAGE_LOCKS = ON
    ) ON [PRIMARY]''
    );

    EXEC(''
    CREATE NONCLUSTERED INDEX IX_' + @info_table + '_3
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@info_table) + ' (Ticker, ListDate)
    WITH (
        STATISTICS_NORECOMPUTE = OFF,
        IGNORE_DUP_KEY = OFF,
        ALLOW_ROW_LOCKS = ON,
        ALLOW_PAGE_LOCKS = ON
    ) ON [PRIMARY]''
    );

    EXEC(''
    CREATE NONCLUSTERED INDEX IX_' + @info_table + '_4
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@info_table) + ' (Ticker, EstablishDate)
    WITH (
        STATISTICS_NORECOMPUTE = OFF,
        IGNORE_DUP_KEY = OFF,
        ALLOW_ROW_LOCKS = ON,
        ALLOW_PAGE_LOCKS = ON
    ) ON [PRIMARY]''
    );

    EXEC(''
    CREATE NONCLUSTERED INDEX IX_' + @info_table + '_5
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@info_table) + ' (Ticker, Exchange)
    WITH (
        STATISTICS_NORECOMPUTE = OFF,
        IGNORE_DUP_KEY = OFF,
        ALLOW_ROW_LOCKS = ON,
        ALLOW_PAGE_LOCKS = ON
    ) ON [PRIMARY]''
    );

    EXEC(''
    CREATE NONCLUSTERED INDEX IX_' + @info_table + '_6
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@info_table) + ' (UpdateTime)
    WITH (
        STATISTICS_NORECOMPUTE = OFF,
        IGNORE_DUP_KEY = OFF,
        ALLOW_ROW_LOCKS = ON,
        ALLOW_PAGE_LOCKS = ON
    ) ON [PRIMARY]''
    );
END
';

-- 执行
EXEC sp_executesql @infotable_sql;

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

