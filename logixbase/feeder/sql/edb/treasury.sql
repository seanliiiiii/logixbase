DECLARE @db NVARCHAR(100) = 'ECON_RESEARCH';
DECLARE @ctd_table NVARCHAR(100) = 'Treasury_CtdData';


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
            [TradeDay] [DATETIME] NOT NULL,
            [Ticker] [NVARCHAR](20) NOT NULL,
            [BondCode] [NVARCHAR](20) NULL,
            [BookInterest] [FLOAT] NULL,
            [TFactor] [FLOAT] NULL,
            [TtlPrice] [FLOAT] NULL,
            [AdjDuration] [FLOAT] NULL,
            [Basis] [FLOAT] NULL,
            [TradePrice] [FLOAT] NULL,
            [FS_Spread] [FLOAT] NULL,
            [IRR] [FLOAT] NULL,
            [NetBasis] [FLOAT] NULL,
            [CTD_IRR] [INT] NULL,
            [CTD_BNOC] [INT] NULL,
            [CTD_MD] [INT] NULL,
            [UpdateTime] DATETIME NOT NULL CONSTRAINT [DF_' + @ctd_table + '_UpdateTime] DEFAULT (GETDATE()),

            CONSTRAINT [PK_' + @ctd_table + '] PRIMARY KEY CLUSTERED ([PK] ASC)
            WITH (PAD_INDEX=OFF, STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF,
                  ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON)
        ) ON [PRIMARY];

        CREATE NONCLUSTERED INDEX [' + @ctd_table + '_1] ON [' + @db + '].[dbo].[' + @ctd_table + '] (TradeDay)
            WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
        CREATE NONCLUSTERED INDEX [' + @ctd_table + '_2] ON [' + @db + '].[dbo].[' + @ctd_table + '] (Ticker)
            WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
        CREATE NONCLUSTERED INDEX [' + @ctd_table + '_3] ON [' + @db + '].[dbo].[' + @ctd_table + '] (UpdateTime)
            WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
        CREATE UNIQUE NONCLUSTERED INDEX [' + @ctd_table + '_4] ON [' + @db + '].[dbo].[' + @ctd_table + '] (TradeDay, Ticker)
            WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=ON, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON) ON [PRIMARY];
    END;
    ';
END

-- 执行拼接后的 SQL
EXEC sp_executesql @sql;
