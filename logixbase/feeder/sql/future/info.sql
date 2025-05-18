DECLARE @db NVARCHAR(100) = 'FUTURE_RESEARCH_DAILY';
DECLARE @basic_info_table NVARCHAR(100) = 'FutureInfo_Basic';
DECLARE @trade_info_table NVARCHAR(100) = 'FutureInfo_Trade';

DECLARE @basic_info NVARCHAR(MAX);
DECLARE @trade_info NVARCHAR(MAX);

-- 期货基础信息表
SET @basic_info = '
IF NOT EXISTS (
    SELECT 1 FROM ' + QUOTENAME(@db) + '.sys.objects
    WHERE name = N''' + @basic_info_table + ''' AND type = ''U''
)
BEGIN
    EXEC(''
    CREATE TABLE ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@basic_info_table) + ' (
        [PK] INT NOT NULL IDENTITY (1, 1),
        [Ticker] VARCHAR(10) NOT NULL,
        [Instrument] VARCHAR(10) NOT NULL,
        [Product] VARCHAR(5) NOT NULL,
        [ListDate] DATETIME NULL,
        [DelistDate] DATETIME NULL,
        [DeliverDate] DATETIME NULL,
        [Multiplier] FLOAT NULL,
        [PriceTick] FLOAT NULL,
        [Exchange] VARCHAR(10) NULL,
        [MinMargin] FLOAT NULL,
        [PriceLimit] FLOAT NULL,
        [QuoteUnit] NVARCHAR(20) NULL,
        [MultiplierUnit] NVARCHAR(20) NULL,
        [ClassLevel1] NVARCHAR(20) NULL,
        [ClassLevel2] NVARCHAR(20) NULL,
        [UpdateTime] DATETIME NULL CONSTRAINT DF_' + @basic_info_table + '_UpdateTime DEFAULT (GETDATE()),
        CONSTRAINT PK_' + @basic_info_table + ' PRIMARY KEY CLUSTERED ([PK] ASC)
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
    CREATE NONCLUSTERED INDEX IX_' + @basic_info_table + '_1
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@basic_info_table) + ' (Product)
    WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
    ON [PRIMARY]''
    );

    EXEC(''
    CREATE UNIQUE NONCLUSTERED INDEX IX_' + @basic_info_table + '_2
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@basic_info_table) + ' (Ticker)
    WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = ON, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
    ON [PRIMARY]''
    );

    EXEC(''
    CREATE NONCLUSTERED INDEX IX_' + @basic_info_table + '_3
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@basic_info_table) + ' (Product, ListDate)
    WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
    ON [PRIMARY]''
    );

    EXEC(''
    CREATE NONCLUSTERED INDEX IX_' + @basic_info_table + '_4
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@basic_info_table) + ' (Product, DelistDate)
    WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
    ON [PRIMARY]''
    );

    EXEC(''
    CREATE NONCLUSTERED INDEX IX_' + @basic_info_table + '_5
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@basic_info_table) + ' (Product, DeliverDate)
    WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
    ON [PRIMARY]''
    );

    EXEC(''
    CREATE NONCLUSTERED INDEX IX_' + @basic_info_table + '_6
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@basic_info_table) + ' (UpdateTime)
    WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
    ON [PRIMARY]''
    );
END
';
EXEC sp_executesql @basic_info;

-- 期货交易信息表
SET @trade_info = '
IF NOT EXISTS (
    SELECT 1 FROM ' + QUOTENAME(@db) + '.sys.objects
    WHERE name = N''' + @trade_info_table + ''' AND type = ''U''
)
BEGIN
    EXEC(''
    CREATE TABLE ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@trade_info_table) + ' (
        [PK] INT NOT NULL IDENTITY(1, 1),
        [Ticker] VARCHAR(10) NOT NULL,
        [Instrument] VARCHAR(10) NOT NULL,
        [Product] VARCHAR(5) NOT NULL,
        [ExecDate] DATETIME NOT NULL,
        [LongMargin] FLOAT NULL,
        [ShortMargin] FLOAT NULL,
        [OpenCommission_Pct] FLOAT NULL,
        [OpenCommission_Fix] FLOAT NULL,
        [CloseCommission_Pct] FLOAT NULL,
        [CloseCommission_Fix] FLOAT NULL,
        [IntradayOpenCommission_Pct] FLOAT NULL,
        [IntradayOpenCommission_Fix] FLOAT NULL,
        [IntradayCloseCommission_Pct] FLOAT NULL,
        [IntradayCloseCommission_Fix] FLOAT NULL,
        [Exchange] VARCHAR(10) NULL,
        [UpdateTime] DATETIME NULL CONSTRAINT DF_' + @trade_info_table + '_UpdateTime DEFAULT (GETDATE()),
        CONSTRAINT PK_' + @trade_info_table + ' PRIMARY KEY CLUSTERED ([PK] ASC)
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
    CREATE NONCLUSTERED INDEX IX_' + @trade_info_table + '_1
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@trade_info_table) + ' (Product)
    WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
    ON [PRIMARY]''
    );

    EXEC(''
    CREATE NONCLUSTERED INDEX IX_' + @trade_info_table + '_2
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@trade_info_table) + ' (Ticker)
    WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
    ON [PRIMARY]''
    );

    EXEC(''
    CREATE UNIQUE NONCLUSTERED INDEX IX_' + @trade_info_table + '_3
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@trade_info_table) + ' (Ticker, ExecDate)
    WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = ON, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
    ON [PRIMARY]''
    );

    EXEC(''
    CREATE NONCLUSTERED INDEX IX_' + @trade_info_table + '_4
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@trade_info_table) + ' (Product, ExecDate)
    WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
    ON [PRIMARY]''
    );

    EXEC(''
    CREATE NONCLUSTERED INDEX IX_' + @trade_info_table + '_5
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@trade_info_table) + ' (UpdateTime)
    WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
    ON [PRIMARY]''
    );

    EXEC(''
    CREATE NONCLUSTERED INDEX IX_' + @trade_info_table + '_6
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@trade_info_table) + ' (Product, UpdateTime)
    WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
    ON [PRIMARY]''
    );
END
';

EXEC sp_executesql @trade_info;