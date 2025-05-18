DECLARE @db NVARCHAR(100) = 'FUTURE_RESEARCH_DAILY';      -- 数据库名
DECLARE @mt_hot NVARCHAR(100) = 'FutureMainTicker';     -- 表名
DECLARE @mt_calendar NVARCHAR(100) = 'FutureMainTicker_Calendar';     -- 表名
DECLARE @mt_all NVARCHAR(100) = 'FutureMainTicker_AllParam'; -- 设置表名
DECLARE @adj_hot NVARCHAR(100) = 'FutureCoefAdj_MainTicker';           -- 目标表名
DECLARE @adj_calendar NVARCHAR(100) = 'FutureCoefAdj_Calendar';           -- 目标表名

DECLARE @mainticker_hot NVARCHAR(MAX);
DECLARE @mainticker_calendar NVARCHAR(MAX);
DECLARE @mainticker_all NVARCHAR(MAX);
DECLARE @coefadj_hot NVARCHAR(MAX);
DECLARE @coefadj_calendar NVARCHAR(MAX);


SET @mainticker_hot = '
IF NOT EXISTS (
    SELECT 1 FROM ' + QUOTENAME(@db) + '.sys.objects
    WHERE name = N''' + @mt_hot + ''' AND type = ''U''
)
BEGIN
    EXEC(''
    CREATE TABLE ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@mt_hot) + ' (
        [PK] INT NOT NULL IDENTITY(1, 1),
        [TradeDay] DATETIME NOT NULL,
        [Product] VARCHAR(5) NOT NULL,
        [Ticker] VARCHAR(10) NOT NULL,
        [UpdateTime] DATETIME NOT NULL CONSTRAINT DF_' + @mt_hot + '_UpdateTime DEFAULT (GETDATE()),
        CONSTRAINT PK_' + @mt_hot + ' PRIMARY KEY CLUSTERED ([PK] ASC)
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
    CREATE NONCLUSTERED INDEX IX_' + @mt_hot + '_1
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@mt_hot) + ' (TradeDay)
    WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
    ON [PRIMARY]''
    );

    EXEC(''
    CREATE NONCLUSTERED INDEX IX_' + @mt_hot + '_2
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@mt_hot) + ' (Product)
    WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
    ON [PRIMARY]''
    );

    EXEC(''
    CREATE NONCLUSTERED INDEX IX_' + @mt_hot + '_3
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@mt_hot) + ' (UpdateTime)
    WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
    ON [PRIMARY]''
    );

    EXEC(''
    CREATE UNIQUE NONCLUSTERED INDEX IX_' + @mt_hot + '_4
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@mt_hot) + ' (TradeDay, Product)
    WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = ON, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
    ON [PRIMARY]''
    );
END
';
EXEC sp_executesql @mainticker_hot;


SET @mainticker_calendar = '
IF NOT EXISTS (
    SELECT 1 FROM ' + QUOTENAME(@db) + '.sys.objects 
    WHERE name = N''' + @mt_calendar + ''' AND type = ''U''
)
BEGIN
    EXEC(''
    CREATE TABLE ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@mt_calendar) + ' (
        [PK] INT NOT NULL IDENTITY(1, 1),
        [TradeDay] DATETIME NOT NULL,
        [Product] VARCHAR(5) NOT NULL,
        [Calendar] VARCHAR(10) NOT NULL,
        [Ticker] VARCHAR(10) NULL,
        [UpdateTime] DATETIME NOT NULL CONSTRAINT DF_' + @mt_calendar + '_UpdateTime DEFAULT (GETDATE()),
        CONSTRAINT PK_' + @mt_calendar + ' PRIMARY KEY CLUSTERED ([PK] ASC)
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
    CREATE NONCLUSTERED INDEX IX_' + @mt_calendar + '_1 
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@mt_calendar) + ' (TradeDay)
    WITH (
        STATISTICS_NORECOMPUTE = OFF, 
        IGNORE_DUP_KEY = OFF, 
        ALLOW_ROW_LOCKS = ON, 
        ALLOW_PAGE_LOCKS = ON
    )
    ON [PRIMARY]''
    );

    EXEC(''
    CREATE NONCLUSTERED INDEX IX_' + @mt_calendar + '_2 
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@mt_calendar) + ' (Product)
    WITH (
        STATISTICS_NORECOMPUTE = OFF, 
        IGNORE_DUP_KEY = OFF, 
        ALLOW_ROW_LOCKS = ON, 
        ALLOW_PAGE_LOCKS = ON
    )
    ON [PRIMARY]''
    );

    EXEC(''
    CREATE UNIQUE NONCLUSTERED INDEX IX_' + @mt_calendar + '_5 
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@mt_calendar) + ' (TradeDay, Product, Calendar)
    WITH (
        STATISTICS_NORECOMPUTE = OFF, 
        IGNORE_DUP_KEY = ON, 
        ALLOW_ROW_LOCKS = ON, 
        ALLOW_PAGE_LOCKS = ON
    )
    ON [PRIMARY]''
    );

    EXEC(''
    CREATE NONCLUSTERED INDEX IX_' + @mt_calendar + '_7 
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@mt_calendar) + ' (UpdateTime)
    WITH (
        STATISTICS_NORECOMPUTE = OFF, 
        IGNORE_DUP_KEY = OFF, 
        ALLOW_ROW_LOCKS = ON, 
        ALLOW_PAGE_LOCKS = ON
    )
    ON [PRIMARY]''
    );
END
';
EXEC sp_executesql @mainticker_calendar;


SET @mainticker_all = '
IF NOT EXISTS (
    SELECT 1 FROM ' + QUOTENAME(@db) + '.sys.objects
    WHERE name = N''' + @mt_all + ''' AND type = ''U''
)
BEGIN
    EXEC(''
    CREATE TABLE ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@mt_all) + ' (
        [PK] INT NOT NULL IDENTITY(1, 1),
        [TradeDay] DATETIME NOT NULL,
        [Product] VARCHAR(5) NOT NULL,
        [Ticker] VARCHAR(10) NOT NULL,
        [Parameter] VARCHAR(50) NOT NULL,
        [UpdateTime] DATETIME NOT NULL CONSTRAINT DF_' + @mt_all + '_UpdateTime DEFAULT (GETDATE()),
        CONSTRAINT PK_' + @mt_all + ' PRIMARY KEY CLUSTERED ([PK] ASC)
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
    CREATE NONCLUSTERED INDEX IX_' + @mt_all + '_1
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@mt_all) + ' (TradeDay)
    WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
    ON [PRIMARY]''
    );

    EXEC(''
    CREATE NONCLUSTERED INDEX IX_' + @mt_all + '_2
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@mt_all) + ' (Product)
    WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
    ON [PRIMARY]''
    );

    EXEC(''
    CREATE NONCLUSTERED INDEX IX_' + @mt_all + '_3
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@mt_all) + ' (UpdateTime)
    WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
    ON [PRIMARY]''
    );

    EXEC(''
    CREATE NONCLUSTERED INDEX IX_' + @mt_all + '_4
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@mt_all) + ' (TradeDay, Product)
    WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
    ON [PRIMARY]''
    );

    EXEC(''
    CREATE NONCLUSTERED INDEX IX_' + @mt_all + '_5
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@mt_all) + ' (Parameter)
    WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
    ON [PRIMARY]''
    );

    EXEC(''
    CREATE NONCLUSTERED INDEX IX_' + @mt_all + '_6
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@mt_all) + ' (TradeDay, Parameter)
    WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
    ON [PRIMARY]''
    );

    EXEC(''
    CREATE UNIQUE NONCLUSTERED INDEX IX_' + @mt_all + '_7
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@mt_all) + ' (TradeDay, Product, Parameter)
    WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = ON, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
    ON [PRIMARY]''
    );
END
';

-- 执行最终 SQL 脚本
EXEC sp_executesql @mainticker_all;


SET @coefadj_hot = '
IF NOT EXISTS (
    SELECT 1 FROM ' + QUOTENAME(@db) + '.sys.objects
    WHERE name = N''' + @adj_hot + ''' AND type = ''U''
)
BEGIN
    EXEC(''
    CREATE TABLE ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@adj_hot) + ' (
        [PK] INT NOT NULL IDENTITY(1, 1),
        [TradeDay] DATETIME NOT NULL,
        [Product] VARCHAR(20) NOT NULL,
        [Ticker] VARCHAR(10) NOT NULL,
        [CoefAdj] FLOAT NULL,
        [UpdateTime] DATETIME NULL CONSTRAINT DF_' + @adj_hot + '_UpdateTime DEFAULT (GETDATE()),
        CONSTRAINT PK_' + @adj_hot + ' PRIMARY KEY CLUSTERED ([PK] ASC)
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
    CREATE NONCLUSTERED INDEX IX_' + @adj_hot + '_1
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@adj_hot) + ' (TradeDay)
    WITH (
        STATISTICS_NORECOMPUTE = OFF,
        IGNORE_DUP_KEY = OFF,
        ALLOW_ROW_LOCKS = ON,
        ALLOW_PAGE_LOCKS = ON
    )
    ON [PRIMARY]''
    );

    EXEC(''
    CREATE NONCLUSTERED INDEX IX_' + @adj_hot + '_2
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@adj_hot) + ' (Product)
    WITH (
        STATISTICS_NORECOMPUTE = OFF,
        IGNORE_DUP_KEY = OFF,
        ALLOW_ROW_LOCKS = ON,
        ALLOW_PAGE_LOCKS = ON
    )
    ON [PRIMARY]''
    );

    EXEC(''
    CREATE UNIQUE NONCLUSTERED INDEX IX_' + @adj_hot + '_3
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@adj_hot) + ' (TradeDay, Product)
    WITH (
        STATISTICS_NORECOMPUTE = OFF,
        IGNORE_DUP_KEY = ON,
        ALLOW_ROW_LOCKS = ON,
        ALLOW_PAGE_LOCKS = ON
    )
    ON [PRIMARY]''
    );

    EXEC(''
    CREATE NONCLUSTERED INDEX IX_' + @adj_hot + '_4
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@adj_hot) + ' (UpdateTime)
    WITH (
        STATISTICS_NORECOMPUTE = OFF,
        IGNORE_DUP_KEY = OFF,
        ALLOW_ROW_LOCKS = ON,
        ALLOW_PAGE_LOCKS = ON
    )
    ON [PRIMARY]''
    );
END
';

-- 执行
EXEC sp_executesql @coefadj_hot;


SET @coefadj_calendar = '
IF NOT EXISTS (
    SELECT 1 FROM ' + QUOTENAME(@db) + '.sys.objects
    WHERE name = N''' + @adj_calendar + ''' AND type = ''U''
)
BEGIN
    EXEC(''
    CREATE TABLE ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@adj_calendar) + ' (
        [PK] INT NOT NULL IDENTITY(1, 1),
        [TradeDay] DATETIME NOT NULL,
        [Product] VARCHAR(20) NOT NULL,
        [Ticker] VARCHAR(10) NOT NULL,
        [CoefAdj] FLOAT NULL,
        [UpdateTime] DATETIME NULL CONSTRAINT DF_' + @adj_calendar + '_UpdateTime DEFAULT (GETDATE()),
        CONSTRAINT PK_' + @adj_calendar + ' PRIMARY KEY CLUSTERED ([PK] ASC)
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
    CREATE NONCLUSTERED INDEX IX_' + @adj_calendar + '_1
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@adj_calendar) + ' (TradeDay)
    WITH (
        STATISTICS_NORECOMPUTE = OFF,
        IGNORE_DUP_KEY = OFF,
        ALLOW_ROW_LOCKS = ON,
        ALLOW_PAGE_LOCKS = ON
    )
    ON [PRIMARY]''
    );

    EXEC(''
    CREATE NONCLUSTERED INDEX IX_' + @adj_calendar + '_2
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@adj_calendar) + ' (Product)
    WITH (
        STATISTICS_NORECOMPUTE = OFF,
        IGNORE_DUP_KEY = OFF,
        ALLOW_ROW_LOCKS = ON,
        ALLOW_PAGE_LOCKS = ON
    )
    ON [PRIMARY]''
    );

    EXEC(''
    CREATE UNIQUE NONCLUSTERED INDEX IX_' + @adj_calendar + '_3
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@adj_calendar) + ' (TradeDay, Product)
    WITH (
        STATISTICS_NORECOMPUTE = OFF,
        IGNORE_DUP_KEY = ON,
        ALLOW_ROW_LOCKS = ON,
        ALLOW_PAGE_LOCKS = ON
    )
    ON [PRIMARY]''
    );

    EXEC(''
    CREATE NONCLUSTERED INDEX IX_' + @adj_calendar + '_4
    ON ' + QUOTENAME(@db) + '.dbo.' + QUOTENAME(@adj_calendar) + ' (UpdateTime)
    WITH (
        STATISTICS_NORECOMPUTE = OFF,
        IGNORE_DUP_KEY = OFF,
        ALLOW_ROW_LOCKS = ON,
        ALLOW_PAGE_LOCKS = ON
    )
    ON [PRIMARY]''
    );
END
';

-- 执行
EXEC sp_executesql @coefadj_calendar;