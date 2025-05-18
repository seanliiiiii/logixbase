DECLARE @database NVARCHAR(100) = 'FUTURE_RESEARCH_DAILY'
DECLARE @tradedaytable NVARCHAR(100) = 'AssetTradeDate_UTCGMT8_test'
DECLARE @tradetimetable NVARCHAR(100) = 'AssetTradeTime_UTCGMT8_test'

DECLARE @create_td_table NVARCHAR(MAX)
DECLARE @create_tt_table NVARCHAR(MAX)

-- 创建交易日表格
SET @create_td_table = '
IF NOT EXISTS (
    SELECT 1 FROM ' + QUOTENAME(@database) + '.sys.objects
    WHERE name = N''' + @tradedaytable + ''' AND type = ''U''
)
BEGIN
    PRINT ''[INFO] Table not exists. Creating...''

    EXEC(''
    CREATE TABLE ' + QUOTENAME(@database) + '.dbo.' + QUOTENAME(@tradedaytable) + ' (
        [PK] INT NOT NULL IDENTITY(1,1),
        [Date] DATETIME NOT NULL,
        [China] INT NOT NULL,
        [UpdateTime] DATETIME NOT NULL CONSTRAINT DF_' + @tradedaytable + '_UpdateTime DEFAULT (GETDATE()),
        CONSTRAINT PK_' + @tradedaytable + ' PRIMARY KEY CLUSTERED ([PK] ASC)
            WITH (PAD_INDEX=OFF, STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF,
                  ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON)
    ) ON [PRIMARY]''
    )

    EXEC(''
    CREATE UNIQUE NONCLUSTERED INDEX IX_' + @tradedaytable + '_Date
    ON ' + QUOTENAME(@database) + '.dbo.' + QUOTENAME(@tradedaytable) + ' (Date)
    WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=ON, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON)
    ON [PRIMARY]''
    )

    EXEC(''
    CREATE NONCLUSTERED INDEX IX_' + @tradedaytable + '_UpdateTime
    ON ' + QUOTENAME(@database) + '.dbo.' + QUOTENAME(@tradedaytable) + ' (UpdateTime)
    WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON)
    ON [PRIMARY]''
    )
END
'
EXEC sp_executesql @create_td_table

-- 创建交易时间表
SET @create_tt_table = '
IF NOT EXISTS (
    SELECT 1 FROM ' + QUOTENAME(@database) + '.sys.objects
    WHERE name = N''' + @tradetimetable + ''' AND type = ''U''
)
BEGIN
    PRINT ''[INFO] Table not exists. Creating...''

    EXEC(''
    CREATE TABLE ' + QUOTENAME(@database) + '.dbo.' + QUOTENAME(@tradetimetable) + ' (
        [PK] INT NOT NULL IDENTITY(1,1),
        [Date] [DATETIME] NOT NULL,
        [Product] [VARCHAR](20) NOT NULL,
        [Day] [NVARCHAR](100) NOT NULL,
        [Night] [NVARCHAR](100) NOT NULL,
        [UpdateTime] DATETIME NOT NULL CONSTRAINT DF_' + @tradetimetable + '_UpdateTime DEFAULT (GETDATE()),
        CONSTRAINT PK_' + @tradetimetable + ' PRIMARY KEY CLUSTERED ([PK] ASC)
            WITH (PAD_INDEX=OFF, STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF,
                  ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON)
    ) ON [PRIMARY]''
    )

    EXEC(''
    CREATE UNIQUE NONCLUSTERED INDEX IX_' + @tradetimetable + '_Product_Date
    ON ' + QUOTENAME(@database) + '.dbo.' + QUOTENAME(@tradetimetable) + ' (Date, Product)
    WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=ON, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON)
    ON [PRIMARY]''
    )

    EXEC(''
    CREATE NONCLUSTERED INDEX IX_' + @tradetimetable + '_UpdateTime
    ON ' + QUOTENAME(@database) + '.dbo.' + QUOTENAME(@tradetimetable) + ' (UpdateTime)
    WITH (STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF, ALLOW_ROW_LOCKS=ON, ALLOW_PAGE_LOCKS=ON)
    ON [PRIMARY]''
    )
END
'
EXEC sp_executesql @create_tt_table
