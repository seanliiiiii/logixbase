DECLARE @db NVARCHAR(100) = '{{db}}';     -- 替换为你的数据库名
DECLARE @table NVARCHAR(100) = '{{table}}';  -- 替换为你的表名
DECLARE @status_table NVARCHAR(100) = 'UpdateRecord_{{label}}';  -- 状态表固定名


DECLARE @sql NVARCHAR(MAX);

IF LOWER(RIGHT(@db, 5)) = 'daily'
BEGIN
    -- ========== 日线行情建表 ==========
    SET @sql = '
    IF NOT EXISTS (
        SELECT 1 FROM ' + QUOTENAME(@db) + '.sys.objects
        WHERE name = N''' + @table + ''' AND type = ''U''
    )
    BEGIN
        EXEC(''
        CREATE TABLE ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@table) + ' (
            [PK] INT NOT NULL IDENTITY(1, 1),
            [DateTime] DATETIME NOT NULL,
            [TradeDay] DATETIME NOT NULL,
            [BarTime] VARCHAR(5) NOT NULL,
            [Ticker] VARCHAR(20) NOT NULL,
            [Open] FLOAT NULL,
            [Close] FLOAT NULL,
            [High] FLOAT NULL,
            [Low] FLOAT NULL,
            [PrevClose] FLOAT NULL,
            [Volume] FLOAT NULL,
            [Amount] FLOAT NULL,
            [DealNumber] INT NULL,
            [Committee] FLOAT NULL,
            [QuantityRelative] FLOAT NULL,
            [BuyVolume] FLOAT NULL,
            [BuyAmount] FLOAT NULL,
            [SaleVolume] FLOAT NULL,
            [SaleAmount] FLOAT NULL,
            [CommitBuy] FLOAT NULL,
            [CommitSale] FLOAT NULL,
            [UpdateTime] DATETIME NOT NULL CONSTRAINT DF_' + @table + '_UpdateTime DEFAULT (GETDATE()),
            CONSTRAINT PK_' + @table + ' PRIMARY KEY CLUSTERED ([PK] ASC)
                WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
                      ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
        ) ON [PRIMARY]''
        );

        EXEC(''
        CREATE NONCLUSTERED INDEX IX_' + @table + '_1 ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@table) + ' (DateTime)
        WITH (STATISTICS_NORECOMPUTE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
        ON [PRIMARY]'';

        CREATE NONCLUSTERED INDEX IX_' + @table + '_2 ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@table) + ' (BarTime)
        WITH (STATISTICS_NORECOMPUTE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
        ON [PRIMARY]'';

        CREATE NONCLUSTERED INDEX IX_' + @table + '_3 ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@table) + ' (TradeDay)
        WITH (STATISTICS_NORECOMPUTE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
        ON [PRIMARY]'';

        CREATE NONCLUSTERED INDEX IX_' + @table + '_4 ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@table) + ' (Ticker)
        WITH (STATISTICS_NORECOMPUTE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
        ON [PRIMARY]'';

        CREATE NONCLUSTERED INDEX IX_' + @table + '_5 ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@table) + ' (UpdateTime)
        WITH (STATISTICS_NORECOMPUTE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
        ON [PRIMARY]'';

        CREATE NONCLUSTERED INDEX IX_' + @table + '_6 ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@table) + ' (TradeDay, Ticker)
        WITH (STATISTICS_NORECOMPUTE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
        ON [PRIMARY]'';

        CREATE UNIQUE NONCLUSTERED INDEX IX_' + @table + '_7 ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@table) + ' (DateTime, Ticker)
        WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = ON, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
        ON [PRIMARY]'';
    END;

    IF NOT EXISTS (
        SELECT 1 FROM ' + QUOTENAME(@db) + '.sys.objects
        WHERE name = N''' + @status_table + ''' AND type = ''U''
    )
    BEGIN
        EXEC(''
        CREATE TABLE ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@status_table) + ' (
            [PK] INT NOT NULL IDENTITY(1, 1),
            [TradeDay] DATETIME NOT NULL,
            [Ticker] VARCHAR(20) NOT NULL,
            [Success] INT NOT NULL,
            [UpdateTime] DATETIME NOT NULL CONSTRAINT DF_' + @status_table + '_UpdateTime DEFAULT (GETDATE()),
            CONSTRAINT PK_' + @status_table + ' PRIMARY KEY CLUSTERED ([PK] ASC)
                WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
                      ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
        ) ON [PRIMARY]''
        );

        EXEC(''
        CREATE NONCLUSTERED INDEX ' + @status_table + '_1 ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@status_table) + ' (TradeDay)
        WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
        CREATE NONCLUSTERED INDEX ' + @status_table + '_4 ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@status_table) + ' (Ticker)
        WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
        CREATE NONCLUSTERED INDEX ' + @status_table + '_6 ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@status_table) + ' (UpdateTime)
        WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
        CREATE UNIQUE NONCLUSTERED INDEX ' + @status_table + '_10 ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@status_table) + ' (TradeDay, Ticker)
        WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=ON, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
        '');
    END;
    ';
END
ELSE
BEGIN
    -- ========== 分钟线行情建表 ==========
    SET @sql = '
    IF NOT EXISTS (
        SELECT 1 FROM ' + QUOTENAME(@db) + '.sys.objects
        WHERE name = N''' + @table + ''' AND type = ''U''
    )
    BEGIN
        EXEC(''
        CREATE TABLE ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@table) + ' (
            [PK] INT NOT NULL IDENTITY(1, 1),
            [DateTime] DATETIME NOT NULL,
            [TradeDay] DATETIME NOT NULL,
            [BarTime] VARCHAR(5) NOT NULL,
            [Open] FLOAT NULL,
            [Close] FLOAT NULL,
            [High] FLOAT NULL,
            [Low] FLOAT NULL,
            [PrevClose] FLOAT NULL,
            [Volume] FLOAT NULL,
            [Amount] FLOAT NULL,
            [DealNumber] INT NULL,
            [Committee] FLOAT NULL,
            [QuantityRelative] FLOAT NULL,
            [BuyVolume] FLOAT NULL,
            [BuyAmount] FLOAT NULL,
            [SaleVolume] FLOAT NULL,
            [SaleAmount] FLOAT NULL,
            [CommitBuy] FLOAT NULL,
            [CommitSale] FLOAT NULL,
            [UpdateTime] DATETIME NOT NULL CONSTRAINT DF_' + @table + '_UpdateTime DEFAULT (GETDATE()),
            CONSTRAINT PK_' + @table + ' PRIMARY KEY CLUSTERED ([PK] ASC)
                WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
                      ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
        ) ON [PRIMARY]''
        );

        EXEC(''
        CREATE UNIQUE NONCLUSTERED INDEX IX_' + @table + '_1 ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@table) + ' (DateTime)
        WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = ON, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
        ON [PRIMARY]'';

        CREATE NONCLUSTERED INDEX IX_' + @table + '_2 ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@table) + ' (BarTime)
        WITH (STATISTICS_NORECOMPUTE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
        ON [PRIMARY]'';

        CREATE NONCLUSTERED INDEX IX_' + @table + '_3 ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@table) + ' (TradeDay)
        WITH (STATISTICS_NORECOMPUTE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
        ON [PRIMARY]'';

        CREATE NONCLUSTERED INDEX IX_' + @table + '_4 ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@table) + ' (UpdateTime)
        WITH (STATISTICS_NORECOMPUTE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
        ON [PRIMARY]'';

        CREATE UNIQUE NONCLUSTERED INDEX IX_' + @table + '_5 ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@table) + ' (TradeDay, DateTime)
        WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = ON, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
        ON [PRIMARY]'';
    END;
    ';
END

-- 执行最终拼接的 SQL
EXEC sp_executesql @sql;
