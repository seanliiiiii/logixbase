DECLARE @db NVARCHAR(100) = 'ECON_RESEARCH';
DECLARE @table NVARCHAR(100) = '{{table}}';

DECLARE @sql NVARCHAR(MAX);

BEGIN
    SET @sql = '
    IF NOT EXISTS (
        SELECT 1 FROM ' + QUOTENAME(@db) + '.sys.objects
        WHERE name = N''' + @table + ''' AND type = ''U''
    )
    BEGIN
        CREATE TABLE [' + @db + '].[dbo].[' + @table + '] (
              [PK] int NOT NULL IDENTITY (1, 1),
              [ID] [INT] NOT NULL,
              [Class] [NVARCHAR](20) NOT NULL,
              [Description] [NVARCHAR](1000) NULL,
              [Source] [NVARCHAR](20) NOT NULL,
              [Ticker] [NVARCHAR](300) NOT NULL,
              [Frequency] [NVARCHAR](10) NULL,
              [PubDay] [NVARCHAR](10) NULL,
              [Lag] [INT] NOT NULL,
              [Unit] [NVARCHAR](10) NULL,
              [IsRatio] [Int] NULL,
              [Note] [NVARCHAR](1000) NULL,
              [UpdateTime] [DATETIME] NULL,

            CONSTRAINT [PK_' + @table + '] PRIMARY KEY CLUSTERED ([PK] ASC)
            WITH (
                PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
                ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON
            )
        ) ON [PRIMARY];

        CREATE NONCLUSTERED INDEX [' + @table + '_1] ON [' + @db + '].[dbo].[' + @table + '] (Class)
        WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

        CREATE NONCLUSTERED INDEX [' + @table + '_2] ON [' + @db + '].[dbo].[' + @table + '] (Source)
        WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

        CREATE NONCLUSTERED INDEX [' + @table + '_3] ON [' + @db + '].[dbo].[' + @table + '] (Ticker)
        WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

        CREATE NONCLUSTERED INDEX [' + @table + '_4] ON [' + @db + '].[dbo].[' + @table + '] (ID)
        WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

        CREATE NONCLUSTERED INDEX [' + @table + '_5] ON [' + @db + '].[dbo].[' + @table + '] (UpdateTime)
        WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

        CREATE UNIQUE NONCLUSTERED INDEX [' + @table + '_6] ON [' + @db + '].[dbo].[' + @table + '] (Class, Ticker)
        WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = ON, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

    END
    ';
END

-- 执行拼接后的 SQL
EXEC sp_executesql @sql;
