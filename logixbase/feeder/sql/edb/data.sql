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
            [Date] [DATETIME] NOT NULL,
            [EconData] [FLOAT] NOT NULL,
            [UpdateTime] DATETIME NOT NULL CONSTRAINT [DF_' + @table + '_UpdateTime] DEFAULT (GETDATE()),
            CONSTRAINT [PK_' + @table + '] PRIMARY KEY CLUSTERED ([PK] ASC)
            WITH (
                PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
                ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON
            )
        ) ON [PRIMARY];

        CREATE NONCLUSTERED INDEX [' + @table + '_1] ON [' + @db + '].[dbo].[' + @table + '] (ID)
        WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

        CREATE NONCLUSTERED INDEX [' + @table + '_2] ON [' + @db + '].[dbo].[' + @table + '] (Date)
        WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

        CREATE NONCLUSTERED INDEX [' + @table + '_3] ON [' + @db + '].[dbo].[' + @table + '] (UpdateTime)
        WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

        CREATE NONCLUSTERED INDEX [' + @table + '_4] ON [' + @db + '].[dbo].[' + @table + ']  (ID, UpdateTime)
        WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

        CREATE UNIQUE NONCLUSTERED INDEX [' + @table + '_5] ON [' + @db + '].[dbo].[' + @table + '] (ID, Date)
        WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = ON, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

        CREATE UNIQUE NONCLUSTERED INDEX [' + @table + '_6] ON [' + @db + '].[dbo].[' + @table + '] (ID, Date, UpdateTime)
        WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = ON, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];
    END
    ';
END

-- 执行拼接后的 SQL
EXEC sp_executesql @sql;
