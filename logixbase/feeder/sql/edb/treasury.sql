DECLARE @db NVARCHAR(100) = 'ECON_RESEARCH';
DECLARE @ctd_table NVARCHAR(100) = 'Treasury_CtdInfo';
DECLARE @val_table NVARCHAR(100) = 'Treasury_Valuation';
DECLARE @tf_table NVARCHAR(100) = 'Treasury_TFactor';

DECLARE @sql NVARCHAR(MAX);

BEGIN
    SET @sql = '
    IF NOT EXISTS (
        SELECT 1 FROM ' + QUOTENAME(@db) + '.sys.objects
        WHERE name = N''' + CAST(@ctd_table AS NVARCHAR(MAX)) + ''' AND type = ''U''
    )
    BEGIN
        CREATE TABLE [' + @db + '].[dbo].[' + @ctd_table + '] (
            [PK] INT NOT NULL IDENTITY(1, 1),
            [Date] [DATETIME] NOT NULL,
            [Ticker] [NVARCHAR](20) NOT NULL,
            [CTD] [NVARCHAR](20) NULL,
            [IRR] [FLOAT] NULL,
            [CTD_IB] [NVARCHAR](20) NULL,
            [IRR_IB] [FLOAT] NULL,
            [CTD_SH] [NVARCHAR](20) NULL,
            [IRR_SH] [FLOAT] NULL,
            [CTD_SZ] [NVARCHAR](20) NULL,
            [IRR_SZ] [FLOAT] NULL,
            [UpdateTime] DATETIME NOT NULL CONSTRAINT [DF_' + @ctd_table + '_UpdateTime] DEFAULT (GETDATE()),

            CONSTRAINT [PK_' + @ctd_table + '] PRIMARY KEY CLUSTERED ([PK] ASC)
            WITH (PAD_INDEX=OFF, STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF,
                  ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON)
        ) ON [PRIMARY];

        CREATE NONCLUSTERED INDEX [' + @ctd_table + '_1] ON [' + @db + '].[dbo].[' + @ctd_table + '] (Date)
            WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
        CREATE NONCLUSTERED INDEX [' + @ctd_table + '_2] ON [' + @db + '].[dbo].[' + @ctd_table + '] (Ticker)
            WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
        CREATE NONCLUSTERED INDEX [' + @ctd_table + '_3] ON [' + @db + '].[dbo].[' + @ctd_table + '] (UpdateTime)
            WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
        CREATE UNIQUE NONCLUSTERED INDEX [' + @ctd_table + '_4] ON [' + @db + '].[dbo].[' + @ctd_table + '] (Date, Ticker)
            WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=ON, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
    END;

    IF NOT EXISTS (
        SELECT 1 FROM ' + QUOTENAME(@db) + '.sys.objects
        WHERE name = N''' + CAST(@val_table AS NVARCHAR(MAX)) + ''' AND type = ''U''
    )
    BEGIN
        CREATE TABLE [' + @db + '].[dbo].[' + @val_table + '] (
            [PK] INT NOT NULL IDENTITY(1, 1),
            [Date] [DATETIME] NOT NULL,
            [Ticker] [NVARCHAR](20) NOT NULL,
            [Yield_CFETS] [FLOAT] NULL,
            [Clean_CFETS] [FLOAT] NULL,
            [Dirty_CFETS] [FLOAT] NULL,
            [Yield_SHC] [FLOAT] NULL,
            [Clean_SHC] [FLOAT] NULL,
            [Dirty_SHC] [FLOAT] NULL,
            [ModifiedDuration_SHC] [FLOAT] NULL,
            [AccruedInterest_SHC] [FLOAT] NULL,
            [ValueOnBP_SHC] [FLOAT] NULL,
            [Convexity_SHC] [FLOAT] NULL,
            [UpdateTime] DATETIME NOT NULL CONSTRAINT [DF_' + @val_table + '_UpdateTime] DEFAULT (GETDATE()),

            CONSTRAINT [PK_' + @val_table + '] PRIMARY KEY CLUSTERED ([PK] ASC)
            WITH (PAD_INDEX=OFF, STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF,
                  ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON)
        ) ON [PRIMARY];

        CREATE NONCLUSTERED INDEX [' + @val_table + '_1] ON [' + @db + '].[dbo].[' + @val_table + '] (Date)
            WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
        CREATE NONCLUSTERED INDEX [' + @val_table + '_2] ON [' + @db + '].[dbo].[' + @val_table + '] (Ticker)
            WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
        CREATE NONCLUSTERED INDEX [' + @val_table + '_3] ON [' + @db + '].[dbo].[' + @val_table + '] (UpdateTime)
            WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
        CREATE UNIQUE NONCLUSTERED INDEX [' + @val_table + '_4] ON [' + @db + '].[dbo].[' + @val_table + '] (Date, Ticker)
            WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=ON, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
    END;

    IF NOT EXISTS (
        SELECT 1 FROM ' + QUOTENAME(@db) + '.sys.objects
        WHERE name = N''' + CAST(@tf_table AS NVARCHAR(MAX)) + ''' AND type = ''U''
    )
    BEGIN
        CREATE TABLE [' + @db + '].[dbo].[' + @tf_table + '] (
            [PK] INT NOT NULL IDENTITY(1, 1),
            [Ticker] [NVARCHAR](20) NOT NULL,
            [Treasury] [NVARCHAR](20) NOT NULL,
            [TFactor] [FLOAT] NULL,
            [UpdateTime] DATETIME NOT NULL CONSTRAINT [DF_' + @tf_table + '_UpdateTime] DEFAULT (GETDATE()),

            CONSTRAINT [PK_' + @tf_table + '] PRIMARY KEY CLUSTERED ([PK] ASC)
            WITH (PAD_INDEX=OFF, STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF,
                  ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON)
        ) ON [PRIMARY];

        CREATE NONCLUSTERED INDEX [' + @tf_table + '_1] ON [' + @db + '].[dbo].[' + @tf_table + '] (Ticker)
            WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
        CREATE NONCLUSTERED INDEX [' + @tf_table + '_2] ON [' + @db + '].[dbo].[' + @tf_table + '] (UpdateTime)
            WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
        CREATE UNIQUE NONCLUSTERED INDEX [' + @tf_table + '_3] ON [' + @db + '].[dbo].[' + @tf_table + '] (Ticker, Treasury)
            WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=ON, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
    END
    ';
END

-- 执行拼接后的 SQL
EXEC sp_executesql @sql;
