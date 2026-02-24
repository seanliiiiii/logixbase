DECLARE @db NVARCHAR(100) = 'ECON_RESEARCH';
DECLARE @table NVARCHAR(100) = 'Future_ProdIndex';

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
            [Product] [NVARCHAR](20) NOT NULL,
            [Dimension] [NVARCHAR](50) NOT NULL,
            [ID] [INT] NOT NULL,
            [IndexID] [NVARCHAR](300) NOT NULL,
            [Description] [NVARCHAR](500) NOT NULL,
            [Correlation] [INT] NOT NULL,
            [Active] [NVARCHAR](300) NULL,
            [UpdateTime] [DATETIME] NULL,

            CONSTRAINT [PK_' + @table + '] PRIMARY KEY CLUSTERED ([PK] ASC)
            WITH (
                PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
                ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON
            )
        ) ON [PRIMARY];

        CREATE NONCLUSTERED INDEX [' + @table + '_1] ON [' + @db + '].[dbo].[' + @table + '] (Product)
        WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

        CREATE NONCLUSTERED INDEX [' + @table + '_2] ON [' + @db + '].[dbo].[' + @table + '] (Dimension)
        WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

        CREATE NONCLUSTERED INDEX [' + @table + '_3] ON [' + @db + '].[dbo].[' + @table + '] (Product, Dimension)
        WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

        CREATE NONCLUSTERED INDEX [' + @table + '_4] ON [' + @db + '].[dbo].[' + @table + '] (Product, Active)
        WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

        CREATE NONCLUSTERED INDEX [' + @table + '_5] ON [' + @db + '].[dbo].[' + @table + '] (UpdateTime)
        WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

        CREATE UNIQUE NONCLUSTERED INDEX [' + @table + '_6] ON [' + @db + '].[dbo].[' + @table + '] (Product, Dimension, ID)
        WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = ON, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

        CREATE UNIQUE NONCLUSTERED INDEX [' + @table + '_7] ON [' + @db + '].[dbo].[' + @table + '] (Product, IndexID)
        WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = ON, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];
    END
    ';
END

-- 执行拼接后的 SQL
EXEC sp_executesql @sql;
