DECLARE @db NVARCHAR(100) = '{{db}}';
DECLARE @table NVARCHAR(100) = '{{table}}';
DECLARE @status_table NVARCHAR(100) = 'UpdateRecord';
DECLARE @sql NVARCHAR(MAX);

-- 主表建表逻辑（动态 SQL 包裹）
SET @sql = '
IF NOT EXISTS (
    SELECT 1 FROM [' + @db + '].sys.objects 
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

END';
EXEC sp_executesql @sql;

-- 状态表逻辑（仅当 db 不包含 daily）
IF CHARINDEX('daily', LOWER(@db)) = 0
BEGIN
    DECLARE @sql2 NVARCHAR(MAX);
    SET @sql2 = '
    IF NOT EXISTS (
        SELECT 1 FROM [' + @db + '].sys.objects 
        WHERE name = N''' + @status_table + ''' AND type = ''U''
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
    END';
    EXEC sp_executesql @sql2;
END