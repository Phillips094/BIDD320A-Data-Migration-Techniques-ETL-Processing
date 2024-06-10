--*************************************************************************--
-- Title: Module 08 - Reviewing the BI ETL Final's data and structure
-- Author: RRoot
-- Desc: This file demonstrates how you can create and test indexes
-- Change Log: When,Who,What
-- 2030-01-01,RRoot,Created File
--**************************************************************************--

/* 
IMPORTANT: You must create a Linked Server for this scipt to work.

USE [master]
Go
EXEC master.dbo.sp_addlinkedserver 
  @server = N'SQLTEACHING.WESTUS.CLOUDAPP.AZURE.COM'
, @srvproduct=N'SQL Server'

EXEC master.dbo.sp_addlinkedsrvlogin 
  @rmtsrvname=N'SQLTEACHING.WESTUS.CLOUDAPP.AZURE.COM'
, @useself=N'False'
, @locallogin=NULL
, @rmtuser=N'BICert'
, @rmtpassword='BICert'
Go


The follwing task Will be completed for Milestone 02
  Review and Estimate using the "Final Checklist.xlsx” file.
  Connect to the Remote OLTP Databases (Or optionally, restore copies on your computer)
  Create the Data Warehouse.
  Start the Solution Developer Document in Excel.
  Create an ETL script.
*/

-- [5a. Start the Solution Developer Document in Excel.] -- 
-----------------------------------------------------------------------------------------------------------------------
-- The first step in any database project is to examine the data!
-- There are two OLTP databases to look at...

Set NoCount ON; -- Turn of the (4 rows affected) messages

Select [Name] as [Azure Source]
 From [SQLTEACHING.WESTUS.CLOUDAPP.AZURE.COM].[DoctorsSchedules].Sys.Tables;
go
Select [Name] as [UW Source] 
 From [Patients].Sys.Tables;
go
-- And one OLAP database to look at...
Select [Name] as [UW Destination]   
 From [DWClinicReportData].Sys.Tables;
go

-- We need to look at the SOURCE data in each table too!
-- [DimClinics] --
Select * 
 From [SQLTEACHING.WESTUS.CLOUDAPP.AZURE.COM].[DoctorsSchedules].[dbo].[Clinics]; 
 -- Looks OK, but IDs are different! Column Names differnt in DW.
Select * 
 From [Patients].[dbo].[Clinics]; 
 -- Looks OK, Address match in both DBs, but State and Zip are only in DoctorsSchedules DB
Select * 
From [DWClinicReportData].[dbo].[DimClinics];
go

-- [DimDoctors] --
Select * 
 From [SQLTEACHING.WESTUS.CLOUDAPP.AZURE.COM].[DoctorsSchedules].[dbo].[Doctors]; 
 -- IDs match and names match, but some Addresses look suspect! Column Names differnt in DW.
Select * 
 From [Patients].[dbo].[Doctors]; 
 -- Looks OK, data matches in both DB, but more infor in DoctorsSchedules DB
Select * 
 From [DWClinicReportData].[dbo].[DimDoctors];
go

-- [FactDoctorShifts] --
Select Top 5 * 
 From [SQLTEACHING.WESTUS.CLOUDAPP.AZURE.COM].[DoctorsSchedules].[dbo].[DoctorShifts]; 
 -- Looks OK, Column Names differnt in DW
Select Top 5 * 
 From [SQLTEACHING.WESTUS.CLOUDAPP.AZURE.COM].[DoctorsSchedules].[dbo].[Shifts]; 
 -- Times look wierd! Should be 24hr clock not a mix of 12hr and 24hr!
-- Bad data in [dbo].[Shifts] must be changed From bad entries to good entries.
With FixTimeCTE
AS
(
Select 
 ShiftStart
,NewShiftStart = Case ShiftStart
				 When '01:00' Then '13:00'
				 When '05:00' Then '17:00'
				 Else ShiftStart
				 End
,ShiftEnd
,NewShiftEnd = Case ShiftEnd
				When '01:00' Then '12:00'
				When '05:00' Then '17:00'
				Else ShiftEnd
				End
From [SQLTEACHING.WESTUS.CLOUDAPP.AZURE.COM].[DoctorsSchedules].[dbo].[Shifts]
) -- Since this is time data so we can use date time functions.
Select *, [Number of hours] = abs(DateDiff(hh, NewShiftStart, NewShiftEnd))
 From FixTimeCTE
Select * 
 From [DWClinicReportData].[dbo].[FactDoctorShifts];
go


-- [DimPatients] --
Select * 
 From [Patients].[dbo].[Patients];  
 -- First and Last Name combined, some Addresses look suspect! Column Names differnt in DW. SCD columns
Select * 
 From [DWClinicReportData].[dbo].[DimPatients];
go

-- [DimProcedures] --
Select * 
 From [Patients].[dbo].[Procedures]; 
 -- Looks OK, column Names differnt in DW.
Select * 
 From [DWClinicReportData].[dbo].[DimProcedures];
go

-- [FactVisits] --
Select * , [Fixed Null DoctorID] = IsNull(Doctor, 0) 
 From [Patients].[dbo].[Visits] Order By Doctor; 
 -- Lots of Lookup values. Column Names differnt in DW.
Select * 
 From [DWClinicReportData].[dbo].[FactVisits];
go

-- [DimDates] --
Select [Min] = Min([ShiftDate]), [Max] = Max([ShiftDate]) -- Find Date Range
From [SQLTEACHING.WESTUS.CLOUDAPP.AZURE.COM].[DoctorsSchedules].[dbo].[DoctorShifts];

Select [Min] = Min([Date]), [Max] = Max([Date]) -- Find Date Range
 From [Patients].[dbo].[Visits]; 

Select *
 From [DWClinicReportData].[dbo].[DimDates];
go

-- And, the [DWClinicReportData] DESTINATION tables have many design mistakes 
print 'Issue: All zip code columns should have been removed' 
print 'Issue: [DoctorEmailAddress] column should have been removed'
print 'Issue: Should remove Identity From DimDates.DateKey to use a Smart Key'
-- Still, this is what we have to work with so...


-- [Document Your Findings] -- 
-----------------------------------------------------------------------------------------------------------------------
-- I always create developer design documentation using an Excel spreadsheet.
-- I will later create a formal design documentation in a Word docx file. 

-- 1) Get Meta Data From the Sources Databases
-- To create the documentation you can use SQL's System tables to gather MetaData.

-- Microsoft has a number of System tables that gives you MetaData you can use for planning. 
-- One of these is SysObjects.
Select * 
  From [SQLTEACHING.WESTUS.CLOUDAPP.AZURE.COM].[DoctorsSchedules].[DBO].SysObjects; -- Older
-- OR
Select * 
 From [SQLTEACHING.WESTUS.CLOUDAPP.AZURE.COM].[DoctorsSchedules].Sys.Objects; -- Newer

-- Get a list of only the "User" tables, and their Primary and Foreign Keys
Select 
  [NAME]
, [Parent object] = iif(parent_obj = 0, 'NA', Object_Name(parent_obj, DB_ID('Patients')))
--, *  
From [Patients].[DBO].SysObjects 
WHERE xtype in ('u', 'pk', 'f')
ORDER BY parent_obj;
go
-- NOTE: Some functions do NOT work well through a LINKED server!
-- But, you can always use a direct connection in ANOTHER query window
Select 
  [NAME]
, [Parent object] = iif(parent_obj = 0, 'NA', Object_Name(parent_obj, DB_ID('DoctorsSchedules')))
--, *  
From [DoctorsSchedules].[DBO].SysObjects 
WHERE xtype in ('u', 'pk', 'f')
ORDER BY parent_obj;
go
Print 'Demo this working in a new window'


-- Microsoft has Many Meta Data views, here is one just for tables
Use [Patients]; 
go

Select * From Sys.Tables;
go
-- Here is an example that gets the Names of the Columns
Select * From Sys.all_columns;
go

-- Notice that the column Data Types referance a lookup ID, system_type_id
Select * From Sys.types;
go

-- We can combine these three tables to get a custom look at the MetaData overall.
Declare @DatabaseName nVarchar(100)= 'Patients'
Declare @SchemaName nVarchar(100)= '%%%' -- Defaults to all Schemas
Declare @TableName nVarchar(100)= '%%%' -- Defaults to all Tables

Select
[Schema Name] = SCHEMA_NAME(T.[schema_id]) ,
[Table Name] = T.name  ,
[Column Name] = C.name ,
[DataType] = Ty.Name , 
[DataType Max Length ] = C.max_length ,
[DataType precision] = Case When C.precision = 0 Then 'NA' Else LTrim(Str(C.precision)) End, 
[DataType scale] = Case When C.scale = 0 Then 'NA' Else LTrim(Str(C.scale)) End
From sys.TABLES AS T 
INNER JOIN sys.COLUMNS AS C 
  ON T.OBJECT_ID = C.OBJECT_ID
INNER JOIN Sys.TYPES AS Ty
  ON C.system_type_id = Ty.system_type_id
WHERE C.name LIKE @TableName
  AND SCHEMA_NAME(T.[schema_id]) LIKE @SchemaName
ORDER BY [Schema Name], [Table Name]; 
go

-- NOTE: There are a number of columns that we need in our design documents
-- and if we create our SQL code just right it will type a lot of the contents for us!
Select
  [ColumnFullName] = DB_Name() + '.' + C.TABLE_SCHEMA  + '.' +  C.TABLE_NAME  + '.' + COLUMN_NAME
, DataType = Case 
  When DATA_TYPE in ( 'Money', 'Decimal') 
    Then IsNull(DATA_TYPE,'') 
    + ' (' +  Cast(NUMERIC_PRECISION as nvarchar(50)) 
    +  ',' +  Cast(NUMERIC_SCALE as nvarchar(50)) 
    + ' )'
  When DATA_TYPE in ('bit', 'int', 'tinyint','bigint', 'datetime', 'uniqueidentifier') 
    Then IsNull(DATA_TYPE,'') 
  Else  IsNull(DATA_TYPE,'') + ' (' +  Cast(IsNull(CHARACTER_MAXIMUM_LENGTH,'') as nvarchar(50)) + ')'
  End
, IsNullable = IsNull(IS_NULLABLE,'')
--, ORDINAL_POSITION
--, COLUMN_DEFAULT = IsNull(COLUMN_DEFAULT,'')
From [INFORMATION_SCHEMA].[COLUMNS] as C
JOIN [INFORMATION_SCHEMA].[TABLES] as T
  ON C.TABLE_NAME = T.TABLE_NAME
WHERE C.TABLE_SCHEMA in ('dbo') AND C.TABLE_NAME NOT in ('sysdiagrams')
Order by C.TABLE_SCHEMA, C.TABLE_NAME, C.ORDINAL_POSITION
Go

-- We can use code like this to create a custom metadata stored procedure. 
-- One that can be used on multiple databases...
Use [TempDB]
go
Create or Alter
Proc pGetTableMetadataByDatabase
--*************************************************************************--
-- Title: pGetTableMetadataByDatabase 
-- Author: 
-- Desc: This sproc gets metadata about the user tables in a given database
-- Change Log: When,Who,What
-- 2030-03-06,,Created File
--**************************************************************************--
(@DatabaseName nvarchar(100))
As
 Begin -- Code
   Declare @RC int = 0;
   Begin Try
    -- Get Tables, PKs and FKs Info
    Exec('
     Select 
       Name
      ,[Parent object] = iif(parent_obj = 0, '''', Object_Name(parent_obj))
      ,*  
     From ' + @DatabaseName + '..SysObjects 
     Where xtype in (''u'', ''pk'', ''f'')
     Order By  parent_obj
    ');
    -- Get Column Info
    Exec ('
     Select
       [ColumnFullName] = ''' + @DatabaseName + ''' + ''.'' + C.TABLE_SCHEMA  + ''.'' +  C.TABLE_NAME  + ''.'' + COLUMN_NAME
      ,[DataType] = Case 
                    When DATA_TYPE in ( ''Money'', ''Decimal'') 
                      Then IsNull(DATA_TYPE,'''') 
                      + ''('' +  Cast(NUMERIC_PRECISION as nvarchar(50)) 
                      +  '','' +  Cast(NUMERIC_SCALE as nvarchar(50)) 
                      + '')''
                    When DATA_TYPE in (''bit'', ''int'', ''tinyint'',''bigint'', ''datetime'', ''uniqueidentifier'') 
                      Then IsNull(DATA_TYPE,'''') 
                    Else IsNull(DATA_TYPE,'''') + '' ('' +  Cast(IsNull(CHARACTER_MAXIMUM_LENGTH,'''') as nvarchar(50)) + '')''
                    End
     ,[IsNullable] = IsNull(IS_NULLABLE,'''')
     --,[ORDINAL_POSITION]
     --,[COLUMN_DEFAULT] = IsNull(COLUMN_DEFAULT,'''')
    From ' + @DatabaseName + '.[INFORMATION_SCHEMA].[COLUMNS] as C
    JOIN ' + @DatabaseName + '.[INFORMATION_SCHEMA].[TABLES] as T
      ON C.TABLE_NAME = T.TABLE_NAME
    WHERE C.TABLE_SCHEMA in (''dbo'')
    Order by C.TABLE_SCHEMA, C.TABLE_NAME, C.ORDINAL_POSITION
    ');
   Set @RC = +1;
  End Try
  Begin Catch
   Print Error_Message();
   Set @RC = -1;
  End Catch
  Return @RC;
 End -- Code
go

-- Then, Second 
-- use the Metadata sproc on the DBs
Declare @RC int = 0;
Exec @RC = tempdb..pGetTableMetadataByDatabase @DatabaseName = 'Patients';
Print @RC;
Exec @RC = tempdb..pGetTableMetadataByDatabase @DatabaseName = 'DWClinicReportData';
Print @RC;

-- NOTE: This cannot Run through Linked Server and you do not have perms to make a sproc there anyway!

/* 
-- Now, I use these results to create my solution worksheet as follows...
-- 1) Right-Click the results and choose the "Copy With Headers" option in the context menu.
-- 2) Open a Blank Excel Worksheet. 
-- 3) Modify the metadata to paste into your Excel Solution Worksheet.
*/

