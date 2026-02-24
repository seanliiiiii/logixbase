DECLARE @db NVARCHAR(100) = '{{db}}';             -- 替换为你的数据库名
DECLARE @table NVARCHAR(100) = '{{table}}';       -- 分钟线或日线表名
DECLARE @status_table NVARCHAR(100) = 'UpdateRecord'; -- 分钟线状态追踪表

DECLARE @sql NVARCHAR(MAX);

IF LOWER(RIGHT(@db, 5)) = 'daily'
BEGIN
    -- ===== 日线行情建表逻辑 =====
    SET @sql = '
    IF NOT EXISTS (
        SELECT 1 FROM ' + QUOTENAME(@db) + '.sys.objects
        WHERE name = N''' + @table + ''' AND type = ''U''
    )
    BEGIN
        CREATE TABLE [' + @db + '].[dbo].[' + @table + '] (
        [PK] INT NOT NULL IDENTITY (1, 1),
        [DateTime] DATETIME NOT NULL,
        [TradeDay] DATETIME NOT NULL,
        [BarTime] VARCHAR(5) NOT NULL,
        [Ticker] VARCHAR(10) NOT NULL,
        [Product] VARCHAR(5) NOT NULL,
        [Open] FLOAT NULL,
        [Close] FLOAT NULL,
        [High] FLOAT NULL,
        [Low] FLOAT NULL,
        [PrevClose] FLOAT NULL,
        [Settle] FLOAT NULL,
        [PrevSettle] FLOAT NULL,
        [Volume] FLOAT NULL,
        [Amount] FLOAT NULL,
        [OpenInterest] FLOAT NULL,
        [UpdateTime] DATETIME NOT NULL CONSTRAINT [DF_' + @table + '_UpdateTime] DEFAULT (GETDATE()),
        CONSTRAINT [PK_' + @table + '] PRIMARY KEY CLUSTERED ([PK] ASC)
        WITH (PAD_INDEX=OFF, STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF,
              ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON)
    ) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [' + @table + '_1] ON [' + @db + '].[dbo].[' + @table + '] (DateTime)
        WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
    CREATE NONCLUSTERED INDEX [' + @table + '_2] ON [' + @db + '].[dbo].[' + @table + '] (BarTime)
        WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
    CREATE NONCLUSTERED INDEX [' + @table + '_3] ON [' + @db + '].[dbo].[' + @table + '] (TradeDay)
        WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
    CREATE NONCLUSTERED INDEX [' + @table + '_4] ON [' + @db + '].[dbo].[' + @table + '] (Product)
        WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
    CREATE NONCLUSTERED INDEX [' + @table + '_5] ON [' + @db + '].[dbo].[' + @table + '] (Ticker)
        WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
    CREATE NONCLUSTERED INDEX [' + @table + '_6] ON [' + @db + '].[dbo].[' + @table + '] (UpdateTime)
        WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
    CREATE NONCLUSTERED INDEX [' + @table + '_7] ON [' + @db + '].[dbo].[' + @table + '] (TradeDay, Product)
        WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
    CREATE NONCLUSTERED INDEX [' + @table + '_8] ON [' + @db + '].[dbo].[' + @table + '] (TradeDay, Ticker)
        WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
    CREATE NONCLUSTERED INDEX [' + @table + '_9] ON [' + @db + '].[dbo].[' + @table + '] (DateTime, Product)
        WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
    CREATE UNIQUE NONCLUSTERED INDEX [' + @table + '_10] ON [' + @db + '].[dbo].[' + @table + '] (DateTime, Ticker)
        WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=ON, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
    CREATE UNIQUE NONCLUSTERED INDEX [' + @table + '_11] ON [' + @db + '].[dbo].[' + @table + '] (TradeDay, DateTime, Ticker)
        WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=ON, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];

    END
    ';
END
ELSE
BEGIN
    -- ===== 分钟线行情建表逻辑 =====
    SET @sql = '
    IF NOT EXISTS (
        SELECT 1 FROM ' + QUOTENAME(@db) + '.sys.objects
        WHERE name = N''' + @table + ''' AND type = ''U''
    )
    BEGIN
        CREATE TABLE [' + @db + '].[dbo].[' + @table + '] (
        [PK] INT NOT NULL IDENTITY (1, 1),
        [DateTime] DATETIME NOT NULL,
        [TradeDay] DATETIME NOT NULL,
        [BarTime] VARCHAR(5) NOT NULL,
        [Ticker] VARCHAR(10) NOT NULL,
        [Open] FLOAT NULL,
        [Close] FLOAT NULL,
        [High] FLOAT NULL,
        [Low] FLOAT NULL,
        [PrevClose] FLOAT NULL,
        [Settle] FLOAT NULL,
        [PrevSettle] FLOAT NULL,
        [Volume] FLOAT NULL,
        [Amount] FLOAT NULL,
        [OpenInterest] FLOAT NULL,
        [UpdateTime] DATETIME NOT NULL CONSTRAINT [DF_' + @table + '_UpdateTime] DEFAULT (GETDATE()),
        CONSTRAINT [PK_' + @table + '] PRIMARY KEY CLUSTERED ([PK] ASC)
        WITH (PAD_INDEX=OFF, STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF,
              ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON)
        ) ON [PRIMARY];

        CREATE NONCLUSTERED INDEX [' + @table + '_1] ON [' + @db + '].[dbo].[' + @table + '] (DateTime)
            WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
        CREATE NONCLUSTERED INDEX [' + @table + '_2] ON [' + @db + '].[dbo].[' + @table + '] (BarTime)
            WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
        CREATE NONCLUSTERED INDEX [' + @table + '_3] ON [' + @db + '].[dbo].[' + @table + '] (TradeDay)
            WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
        CREATE NONCLUSTERED INDEX [' + @table + '_4] ON [' + @db + '].[dbo].[' + @table + '] (Ticker)
            WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
        CREATE NONCLUSTERED INDEX [' + @table + '_5] ON [' + @db + '].[dbo].[' + @table + '] (UpdateTime)
            WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
        CREATE NONCLUSTERED INDEX [' + @table + '_6] ON [' + @db + '].[dbo].[' + @table + '] (TradeDay, Ticker)
            WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
        CREATE UNIQUE NONCLUSTERED INDEX [' + @table + '_7] ON [' + @db + '].[dbo].[' + @table + '] (DateTime, Ticker)
            WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=ON, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
        CREATE UNIQUE NONCLUSTERED INDEX [' + @table + '_8] ON [' + @db + '].[dbo].[' + @table + '] (TradeDay, DateTime, Ticker)
            WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=ON, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];

    END;

    IF NOT EXISTS (
        SELECT 1 FROM ' + QUOTENAME(@db) + '.sys.objects
        WHERE name = N''' + CAST(@status_table AS NVARCHAR(MAX)) + ''' AND type = ''U''
    )
    BEGIN
        CREATE TABLE [' + @db + '].[dbo].[' + @status_table + '] (
            [PK] INT NOT NULL IDENTITY(1, 1),
            [TradeDay] DATETIME NOT NULL,
            [Product] VARCHAR(5) NOT NULL,
            [Success] INT NOT NULL,
            [UpdateTime] DATETIME NOT NULL CONSTRAINT [DF_' + @status_table + '_UpdateTime] DEFAULT (GETDATE()),
            CONSTRAINT [PK_' + @status_table + '] PRIMARY KEY CLUSTERED ([PK] ASC)
            WITH (PAD_INDEX=OFF, STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF,
                  ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON)
        ) ON [PRIMARY];

        CREATE NONCLUSTERED INDEX [' + @status_table + '_1] ON [' + @db + '].[dbo].[' + @status_table + '] (TradeDay)
            WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
        CREATE NONCLUSTERED INDEX [' + @status_table + '_4] ON [' + @db + '].[dbo].[' + @status_table + '] (Product)
            WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
        CREATE NONCLUSTERED INDEX [' + @status_table + '_6] ON [' + @db + '].[dbo].[' + @status_table + '] (UpdateTime)
            WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
        CREATE UNIQUE NONCLUSTERED INDEX [' + @status_table + '_10] ON [' + @db + '].[dbo].[' + @status_table + '] (TradeDay, Product)
            WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=ON, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
    END
    ';
END

-- 执行拼接后的 SQL
EXEC sp_executesql @sql;