DECLARE @db VARCHAR(20)
DECLARE @size VARCHAR(10)
DECLARE @growth VARCHAR(10)

SET @db='INDEX_RESEARCH_DAILY'
SET @size='100MB'
SET @growth='50MB'

DECLARE @sql VARCHAR(MAX)

BEGIN
SET @sql='
		 CREATE DATABASE ' + @db + 
		 ' ON 
		   (NAME = ' +@db+ '_1, 
			FILENAME = ''D:\Program Files\Microsoft SQL Server\MSSQL15.MSSQLSERVER\MSSQL\DATA\'+@db+'_1.mdf'',
			SIZE = ' + @size + ',
			FILEGROWTH = ' + @growth + '),
			(NAME = ' +@db+ '_2, 
			FILENAME = ''E:\Microsoft SQL Server\'+@db+'_2.ndf'',
			SIZE = ' + @size + ',
			FILEGROWTH = ' + @growth + '),
			(NAME = ' +@db+ '_3, 
			FILENAME = ''E:\Microsoft SQL Server\'+@db+'_3.ndf'',
			SIZE = ' + @size + ',
			FILEGROWTH = ' + @growth + '),
			(NAME = ' +@db+ '_4, 
			FILENAME = ''F:\Microsoft SQL Server\'+@db+'_4.ndf'',
			SIZE = ' + @size + ',
			FILEGROWTH = ' + @growth + '),
			(NAME = ' +@db+ '_5, 
			FILENAME = ''F:\Microsoft SQL Server\'+@db+'_5.ndf'',
			SIZE = ' + @size + ',
			FILEGROWTH = ' + @growth + '),
			(NAME = ' +@db+ '_6, 
			FILENAME = ''G:\Microsoft SQL Server\'+@db+'_6.ndf'',
			SIZE = ' + @size + ',
			FILEGROWTH = ' + @growth + '),			
			(NAME = ' +@db+ '_7, 
			FILENAME = ''G:\Microsoft SQL Server\'+@db+'_7.ndf'',
			SIZE = ' + @size + ',
			FILEGROWTH = ' + @growth + '),
			(NAME = ' +@db+ '_8, 
			FILENAME = ''H:\Microsoft SQL Server\'+@db+'_8.ndf'',
			SIZE = ' + @size + ',
			FILEGROWTH = ' + @growth + '),
			(NAME = ' +@db+ '_9, 
			FILENAME = ''H:\Microsoft SQL Server\'+@db+'_9.ndf'',
			SIZE = ' + @size + ',
			FILEGROWTH = ' + @growth + '),
			(NAME = ' +@db+ '_10, 
			FILENAME = ''I:\Microsoft SQL Server\'+@db+'_10.ndf'',
			SIZE = ' + @size + ',
			FILEGROWTH = ' + @growth + ')

			LOG ON
		   (NAME = ' +@db+ '_log, 
			FILENAME = ''D:\Program Files\Microsoft SQL Server\MSSQL15.MSSQLSERVER\MSSQL\DATA\'+@db+'_log.ldf'',
			SIZE = ' + @size + ',
			MAXSIZE = 5120MB,
			FILEGROWTH = ' + @growth + ')'

exec(@sql)
END