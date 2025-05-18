-- 声明数据库和表名变量
DECLARE @db NVARCHAR(100) = 'OPTION_RESEARCH_DAILY';
DECLARE @info_table NVARCHAR(100) = 'OptionInfo_Basic';

-- 构造完整 SQL 字符串
DECLARE @infotable_sql NVARCHAR(MAX);
SET @infotable_sql = '
IF NOT EXISTS (
    SELECT name FROM [' + @db + '].sys.objects WHERE name = ''' + @info_table + ''' AND type = ''U''
)
BEGIN
    EXEC(''
    CREATE TABLE [' + @db + '].[dbo].[' + @info_table + '] (
        [PK] INT NOT NULL IDENTITY (1, 1),
        [Ticker] VARCHAR(20) NOT NULL,
        [Instrument] VARCHAR(20) NOT NULL,
        [Product] VARCHAR(10) NOT NULL,
        [Underlying] VARCHAR(20) NOT NULL,
        [UnderlyingClass] VARCHAR(20) NOT NULL,
        [Adjusted] VARCHAR(20) NULL,
        [ExerciseType] VARCHAR(20) NOT NULL,
        [CallorPut] VARCHAR(20) NOT NULL,
        [Multiplier] FLOAT NOT NULL,
        [Strike] FLOAT NOT NULL,
        [ListDate] DATETIME NOT NULL,
        [DelistDate] DATETIME NOT NULL,
        [ExpiryDate] DATETIME NOT NULL,
        [PriceTick] FLOAT NOT NULL,
        [Exchange] VARCHAR(20) NULL,
        [ExecDate] DATETIME NOT NULL,
        [UpdateTime] DATETIME NULL CONSTRAINT [DF_' + @info_table + '_UpdateTime] DEFAULT (GETDATE()),
        CONSTRAINT [PK_' + @info_table + '] PRIMARY KEY CLUSTERED ([PK] ASC)
            WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
                  ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON),
        CONSTRAINT [UC_' + @info_table + '] UNIQUE (Ticker, ExerciseType, Multiplier, Strike, PriceTick)
    ) ON [PRIMARY]'');

    EXEC(''
    CREATE NONCLUSTERED INDEX ' + @info_table + '_1 ON [' + @db + '].[dbo].[' + @info_table + '] (Ticker)
        WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX ' + @info_table + '_2 ON [' + @db + '].[dbo].[' + @info_table + '] (Product)
        WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX ' + @info_table + '_3 ON [' + @db + '].[dbo].[' + @info_table + '] (Adjusted)
        WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX ' + @info_table + '_4 ON [' + @db + '].[dbo].[' + @info_table + '] (UnderlyingClass)
        WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX ' + @info_table + '_5 ON [' + @db + '].[dbo].[' + @info_table + '] (Underlying)
        WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX ' + @info_table + '_6 ON [' + @db + '].[dbo].[' + @info_table + '] (Multiplier)
        WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX ' + @info_table + '_7 ON [' + @db + '].[dbo].[' + @info_table + '] (Exchange)
        WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX ' + @info_table + '_8 ON [' + @db + '].[dbo].[' + @info_table + '] (ListDate, DelistDate, Strike, CallorPut)
        WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];
    '');
END
';

-- 执行完整拼接 SQL
EXEC sp_executesql @infotable_sql;
