/************************************************************************************************
Title: ETL Final Project: DWClinicReportData Load
Desc: This file contains ETL code for BI ETL Final.
Dev: FGomez
Date: 03/07/2024
Change Log: (When, Who, What)
			3/11/2020,RRoot, Separated Pre-load, Load, and Post-load ETL code for clarity.
			3/11/2020,RRoot, Added ETL insert code for all tables.
			3/97/2024,FGomez, Added ETL insert code for all tables in our DWClinicReportData Data Warehouse.

IMPORTANT: You must create a Linked Server for this script to work.

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

 -- AND

USE [master]
GO
EXEC master.dbo.sp_addlinkedserver 
  @server = N'SQLTEACHING.WESTUS.CLOUDAPP.AZURE.COM'
, @srvproduct=N'SQL Server'

GO
USE [master]
GO
EXEC master.dbo.sp_addlinkedsrvlogin 
  @rmtsrvname = N'SQLTEACHING.WESTUS.CLOUDAPP.AZURE.COM'
, @locallogin = NULL 
, @useself = N'False'
, @rmtuser = N'BICert'
, @rmtpassword = N'BICert'
GO

************************************************************************************************/

Set NoCount ON; -- Turn of the (4 rows affected) messages
go
Use DWClinicReportData;
go
/**************** Load Staging Tables Tables ********************************************/


If (Object_ID('StagedClinics') is not null) Drop Table StagedClinics;
go
Select *
 Into StagedClinics
 From [SQLTEACHING.WESTUS.CLOUDAPP.AZURE.COM].[DoctorsSchedules].[dbo].[Clinics]; 
go
Select * From StagedClinics;
go

If (Object_ID('StagedDoctors') is not null) Drop Table StagedDoctors;
go
Select *
 Into StagedDoctors
 From [SQLTEACHING.WESTUS.CLOUDAPP.AZURE.COM].[DoctorsSchedules].[dbo].[Doctors]; 
go
Select * From StagedDoctors;
go

If (Object_ID('StagedDoctorShifts') is not null) Drop Table StagedDoctorShifts;
go
Select *
 Into StagedDoctorShifts
 From [SQLTEACHING.WESTUS.CLOUDAPP.AZURE.COM].[DoctorsSchedules].[dbo].[DoctorShifts]; 
go
Select * From StagedDoctorShifts;
go

If (Object_ID('StagedShifts') is not null) Drop Table StagedShifts;
go
Select *
 Into StagedShifts
 From [SQLTEACHING.WESTUS.CLOUDAPP.AZURE.COM].[DoctorsSchedules].[dbo].[Shifts]; 
go
Select * From StagedShifts;
go

If (Object_ID('StagedPatients') is not null) Drop Table StagedPatients;
go
Select * 
 Into StagedPatients
 From [SQLTEACHING.WESTUS.CLOUDAPP.AZURE.COM].[Patients].[dbo].[Patients];
go
Select * From StagedPatients;
go

If (Object_ID('StagedProcedures') is not null) Drop Table StagedProcedures;
go
Select * 
 Into StagedProcedures
 From [SQLTEACHING.WESTUS.CLOUDAPP.AZURE.COM].[Patients].[dbo].[Procedures];
go
Select * From StagedProcedures;
go

-- We are going to create dates a bit different this time, by using a LOOKUP/DOMAIN table
If Exists(Select Name From TempDB.Sys.Tables Where Name = 'Dates') Drop Table TempDB.dbo.Dates;
go
Set nocount ON;
go

If Exists(Select Name From TempDB.Sys.Tables Where Name = 'LookupDates') Drop Table TempDB.dbo.LookupDates;
go
Create Table TempDB.dbo.LookupDates (DateID int, FullDate date);
go
Declare @StartDate datetime = '01/01/1990', @EndDate datetime = '12/31/2020'; 
Declare @DateInProcess datetime = @StartDate;
While @DateInProcess <= @EndDate
 Begin
  Insert Into TempDB.dbo.LookupDates ( [DateID], [FullDate] )
   Values (Cast(Convert(nVarchar(50), @DateInProcess, 112) as int) , @DateInProcess ); 
  Set @DateInProcess = DateAdd(d, 1, @DateInProcess);
 End
go
Select * from TempDB.dbo.LookupDates; 
go

/**************** Load Dimension Tables ********************************************/
-- DimClinics -----------------------------------------------------------------------------------------------------------------------------
Delete From [dbo].[FactDoctorShifts];
Delete From [dbo].[FactVisits];
Delete From DimClinics;
DBCC CHECKIDENT ('DimClinics', RESEED, 0);
go

/* [dbo].[Clinics] */
If (Select Object_ID('vETLDimClinics')) is NOT null Drop View vETLDimClinics;
go

Create View vETLDimClinics
AS
	Select
	--ClinicKey does not exist in the source DB
	  ClinicID = [ClinicID] -- Note: This table does not use IDs 100,200,300!
	, ClinicName = Cast(isNull([ClinicName],'Missing Data')as nVarchar(100))
	, ClinicCity = Cast(isNull([City],'Missing Data')as nVarchar(100))
	, ClinicState = Cast(isNull([State],'Missing Data')as nVarchar(100))
	, ClinicZip = Cast(isNull([Zip],'Missing Data')as nVarchar(100))
	From StagedClinics -- Using staging tables let's you test your ETL easier (Ins, Upd, Del not allowed on source)
    -- OR
	--From [SQLTEACHING.WESTUS.CLOUDAPP.AZURE.COM].[DoctorsSchedules].[dbo].[Clinics]; 
go
-- Select * From vETLDimClinics

If (Select Object_ID('pETLInsDimClinics')) is NOT null Drop Procedure pETLInsDimClinics;
go
Create Procedure pETLInsDimClinics
AS
/************************************************************************************************
Desc: Inserts Transformed Data into [dbo].[DimClinics]
Dev: FGomez
Date: 03/07/2024
Change Log: (When, Who, What)
************************************************************************************************/
Begin
	Declare @RC int = 0;
	Begin Try 
		-- ETL Processing Code --
			Set NoCount On;
    -- 1) In a Type 1 SCD table its easier to do a DELETE any Updated rows first...
		With ChangedClinics
		As( Select [ClinicID], [ClinicName], [ClinicCity], [ClinicState], [ClinicZip] From vETLDimClinics
			  Except
			Select [ClinicID], [ClinicName], [ClinicCity], [ClinicState], [ClinicZip] From [dbo].[DimClinics]
		)Delete From [DWClinicReportData].[dbo].[DimClinics]     
		  Where [ClinicID] IN (Select [ClinicID] From ChangedClinics)
		;      
    -- 2) ... then add them back for as an new INSERT. This code inserts both new and updated rows! 
		With NewOrChangedClinics
		As(	Select [ClinicID], [ClinicName], [ClinicCity], [ClinicState], [ClinicZip] From vETLDimClinics
			  Except
			Select [ClinicID], [ClinicName], [ClinicCity], [ClinicState], [ClinicZip] From [dbo].[DimClinics]
		) Insert Into [DWClinicReportData].[dbo].[DimClinics]
      ([ClinicID], [ClinicName], [ClinicCity], [ClinicState], [ClinicZip])
      Select [ClinicID], [ClinicName], [ClinicCity], [ClinicState], [ClinicZip]
       From vETLDimClinics
		    Where [ClinicID] IN (Select [ClinicID] From NewOrChangedClinics)
		; 
    -- 3) For Delete, you can either delete the row, or BETTER yet Flag the row as Deleted
		With DeletedClinics 
			As( Select [ClinicID], [ClinicName], [ClinicCity], [ClinicState], [ClinicZip] From [dbo].[DimClinics]
			    Except
			    Select [ClinicID], [ClinicName], [ClinicCity], [ClinicState], [ClinicZip] From vETLDimClinics
   		)Update [DimClinics] -- See demo below on patindex()
        Set [ClinicName] = iif(patindex('%(Deleted)%',[ClinicName]) > 0, [ClinicName], [ClinicName] + ' (Deleted)')            
		     Where [ClinicID] IN (Select [ClinicID] From DeletedClinics)
	   ;
		-- ETL Processing Code --
		Print 'Success in inserting data into [dbo].[DimClinics] ETL Process'
		Set @RC = 1;
  End Try
  Begin Catch 
		Print 'Error inserting data into [dbo].[DimClinics] ETL Process'
		Print ERROR_MESSAGE()
		Set @RC = -1;
  End Catch
  Return @RC;
End
go

--Execute pETLInsDimClinics;
--Select * From DimClinics;
--go

-- Now, we test that the code works!
-- 1) Clear the table and reset the Identity Spec.
--Delete From [dbo].[FactDoctorShifts];
--Delete From [dbo].[FactVisits];
--Delete From [dbo].[DimClinics];
--DBCC CHECKIDENT ('dbo.DimClinics', RESEED, 0); 
--go

--Select * From DimClinics;
--go

---- 2) Test the initial Fill
--Execute pETLInsDimClinics;
--Select * From DimClinics;
--go

---- 3) Test inserting data
--Insert Into StagedClinics 
--([ClinicID], [ClinicName], [Address], [City], [State], [Zip])
--Values(4, 'TestIns', 'TestIns', 'TestIns','TestIns', '98000');
--Select * From StagedClinics;
--go

--Execute pETLInsDimClinics;
--Select * From DimClinics;
--go

---- 4) Test updating data
--Update StagedClinics
-- Set [ClinicName] = 'TestUPDATE' Where ClinicName = 'TestIns';
--Select * From StagedClinics;
--go

--Execute pETLInsDimClinics;
--Select * From DimClinics;
--go

---- 4) Test deleting data
--Delete From StagedClinics 
--  Where ClinicName = 'TestUPDATE';   
--Select * From StagedClinics;
--go

--Execute pETLInsDimClinics;
--Select * From DimClinics;
--go

--Delete From [dbo].[FactDoctorShifts];
--Delete From [dbo].[FactVisits];
--Delete From DimClinics;
--DBCC CHECKIDENT ('DimClinics', RESEED, 0);
--go

-- DimDoctors -----------------------------------------------------------------------------------------------------------------------------
Delete From [dbo].[FactDoctorShifts];
Delete From [dbo].[FactVisits];
Delete From DimDoctors;
DBCC CHECKIDENT ('DimPatients', RESEED, 0);
go

--Select * From StagedDoctors;
--Select * From [DimDoctors];
--go

--Insert Into [dbo].[DimDoctors]
--([DoctorID], [DoctorFullName], [DoctorEmailAddress], [DoctorCity], [DoctorState], [DoctorZip])
--Select 
-- [DoctorID]
--,[DoctorFullName] = [FirstName] + ' ' + [LastName]
--,[DoctorEmailAddress] = [EmailAddress]
--,[DoctorCity] =  LTrim([City])
--,[DoctorState] = Replace(Replace([State],' ', ''),'Redmond','')
--,[DoctorZip] = [Zip]
--From StagedDoctors;
---- Could have done this -> From [SQLTEACHING.WESTUS.CLOUDAPP.AZURE.COM].[DoctorsSchedules].[dbo].[Doctors];
--go

/* [dbo].[Doctors] */
If (Select Object_ID('vETLDimDoctors')) is NOT null Drop View vETLDimDoctors;
go

CREATE VIEW vETLDimDoctors AS
SELECT
	 DoctorID
	,DoctorFullName = CAST(ISNULL(([FirstName] + ' ' + [LastName]), 'Check') AS nVarchar(200))
	,DoctorEmailAddress = CAST(ISNULL([EmailAddress], 'Check') AS nVarchar(100))
	,DoctorCity = CAST(ISNULL([City], 'Check') AS nVarchar(100))
	,DoctorState = CAST(ISNULL([State], 'Check') AS nVarchar(100))
	,DoctorZip = CAST(ISNULL([Zip], 'Check') AS nVarchar(5))
FROM StagedDoctors;
go
-- Select * From vETLDimDoctors

If (Select Object_ID('pETLInsDimDoctors')) is NOT null Drop Procedure pETLInsDimDoctors;
go
Create Procedure pETLInsDimDoctors
AS
/************************************************************************************************
Desc: Inserts Transformed Data into [dbo].[DimDoctors]
Dev: FGomez
Date: 03/07/2024
Change Log: (When, Who, What)
************************************************************************************************/
Begin
	Declare @RC int = 0;
	Begin Try 
		-- ETL Processing Code --
			Set NoCount On;
    -- 1) In a Type 1 SCD table its easier to do a DELETE any Updated rows first...
		With ChangedDoctors
		As( Select [DoctorID], [DoctorFullName], [DoctorEmailAddress], [DoctorCity], [DoctorState], [DoctorZip] From vETLDimDoctors
			  Except
			Select [DoctorID], [DoctorFullName], [DoctorEmailAddress], [DoctorCity], [DoctorState], [DoctorZip] From [dbo].[DimDoctors]
		)Delete From [DWClinicReportData].[dbo].[DimDoctors]     
		  Where [DoctorID] IN (Select [DoctorID] From ChangedDoctors)
		;      
    -- 2) ... then add them back for as an new INSERT. This code inserts both new and updated rows! 
		With NewOrChangedDoctors
		As(	Select [DoctorID], [DoctorFullName], [DoctorEmailAddress], [DoctorCity], [DoctorState], [DoctorZip] From vETLDimDoctors
			  Except
			Select [DoctorID], [DoctorFullName], [DoctorEmailAddress], [DoctorCity], [DoctorState], [DoctorZip] From [dbo].[DimDoctors]
		) Insert Into [DWClinicReportData].[dbo].[DimDoctors]
      ([DoctorID], [DoctorFullName], [DoctorEmailAddress], [DoctorCity], [DoctorState], [DoctorZip])
      Select [DoctorID], [DoctorFullName], [DoctorEmailAddress], [DoctorCity], [DoctorState], [DoctorZip]
       From vETLDimDoctors
		    Where [DoctorID] IN (Select [DoctorID] From NewOrChangedDoctors)
		; 
    -- 3) For Delete, you can either delete the row, or BETTER yet Flag the row as Deleted
		With DeletedDoctors
			As( Select [DoctorID], [DoctorFullName], [DoctorEmailAddress], [DoctorCity], [DoctorState], [DoctorZip] From [dbo].[DimDoctors]
			    Except
			    Select [DoctorID], [DoctorFullName], [DoctorEmailAddress], [DoctorCity], [DoctorState], [DoctorZip] From vETLDimDoctors
   		)Update [DimDoctors] -- See demo below on patindex()
        Set [DoctorFullName] = iif(patindex('%(Deleted)%',[DoctorFullName]) > 0, [DoctorFullName], [DoctorFullName] + ' (Deleted)')            
		     Where [DoctorID] IN (Select [DoctorID] From DeletedDoctors)
	   ;
		-- ETL Processing Code --
		Print 'Success in inserting data into [dbo].[DimDoctors] ETL Process'
		Set @RC = 1;
  End Try
  Begin Catch 
		Print 'Error inserting data into [dbo].[DimDoctors] ETL Process'
		Print ERROR_MESSAGE()
		Set @RC = -1;
  End Catch
  Return @RC;
End
go

--Execute pETLInsDimDoctors
--Select * from DimDoctors;
--go 

--Delete From [dbo].[FactDoctorShifts];
--Delete From [dbo].[FactVisits];
--Delete From DimDoctors;
--DBCC CHECKIDENT ('DimPatients', RESEED, 0);
--go

-- DimPatients -----------------------------------------------------------------------------------------------------------------------------
Delete From [dbo].[FactDoctorShifts];
Delete From [dbo].[FactVisits];
Delete From DimPatients;
DBCC CHECKIDENT ('DimPatients', RESEED, 0);
go

--Select * From StagedPatients;
--Select * From [DimPatients];
--go

--Insert Into [dbo].[DimPatients] 
--([PatientID]
--,[PatientFullName]
--,[PatientCity]
--,[PatientState]
--,[PatientZipCode]
--,[StartDate]
--,[EndDate]
--,[IsCurrent]
--) Select 
--   [PatientID] = [ID]
--  ,[PatientFullName] = [FName] + ' ' + [LName]
--  ,[PatientCity] = [City]
--  ,[PatientState] = [State]
--  ,[PatientZipCode] = [ZipCode]
--  ,[StartDate] = GetDate()
--  ,[EndDate] = Null
--  ,[IsCurrent]= 1
--  From StagedPatients
--  Order By ID;
--go
--select * from DimPatients

/* [dbo].[Patients] */
If (Select Object_ID('vETLDimPatients')) is NOT null Drop View vETLDimPatients;
go

CREATE VIEW vETLDimPatients AS
SELECT
	 PatientID = CAST(ID AS int)
	,PatientFullName = CAST(ISNULL(([FName] + ' ' + [LName]), 'Check') AS nVarchar(100))
	,PatientCity = CAST(ISNULL([City], 'Check') AS nVarchar(100))
	,PatientState = CAST(ISNULL([State], 'Check') AS nVarchar(100))
	,PatientZipCode = CAST(ISNULL([ZipCode], -1) AS int)
	,IsCurrent = 1
FROM StagedPatients;
go
-- Select * From vETLDimPatients

If (Select Object_ID('pETLInsDimPatients')) is NOT null Drop Procedure pETLInsDimPatients;
go
Create Procedure pETLInsDimPatients
AS
/************************************************************************************************
Desc: Inserts Transformed Data into [dbo].[DimPatients]
Dev: FGomez
Date: 03/07/2024
Change Log: (When, Who, What)
************************************************************************************************/
Begin
	Declare @RC int = 0;
	Begin Try 
		-- ETL Processing Code --
			Set NoCount On;
		Begin Tran;
			  Merge Into DimPatients as t
			   Using vETLDimPatients as s -- For Merge to work with SCD tables, I need to insert a new row when the following is not true:
				On  t.PatientID = s.PatientID
				And t.PatientFullName = s.PatientFullName
				And t.PatientCity = s.PatientCity
				And t.PatientState = s.PatientState
				And t.PatientZipCode = s.PatientZipCode
				And t.IsCurrent = s.IsCurrent -- Added to capture row where all but this is a match. This when all is the same, the the is current status then       
			   When Not Matched -- At least one column value does not match add a new row:
				Then
				 Insert (PatientID, PatientFullName, PatientCity, PatientState, PatientZipCode, StartDate, EndDate, IsCurrent)
				  Values (s.PatientID
						,s.PatientFullName
						,s.PatientCity
						,s.PatientState
						,s.PatientZipCode
						,CONVERT(DATE, GetDate()) -- Smart Key can be joined to the DimDate
						,Null
						,1)
				When Not Matched By Source -- If there is a row in the target (dim) table that is no longer in the source table
				 Then -- indicate that row is no longer current
				  Update 
				   Set t.EndDate = CONVERT(DATE, GetDate()) -- Smart Key can join to the DimDate
					  ,t.IsCurrent = 0;
			Commit Tran;
			-- ETL Logging Code --
			Exec pInsETLLog
					@ETLAction = 'pETLSyncDimPatients'
				   ,@ETLLogMessage = 'DimPatients synced';
			Set @RC = +1
		  End Try
		  Begin Catch
			 Declare @ErrorMessage nvarchar(1000) = Error_Message();
			 Exec pInsETLLog 
				  @ETLAction = 'pETLSyncDimPatients'
				 ,@ETLLogMessage = @ErrorMessage;
			Set @RC = -1
		  End Catch
		  Return @RC;
End
go

--Exec pETLInsDimPatients
--Select * from [dbo].[DimPatients];
--go 

--Delete From [dbo].[FactDoctorShifts];
--Delete From [dbo].[FactVisits];
--Delete From DimPatients;
--DBCC CHECKIDENT ('DimPatients', RESEED, 0);
--go

-- DimProcedures -----------------------------------------------------------------------------------------------------------------------------
Delete From [dbo].[FactDoctorShifts];
Delete From [dbo].[FactVisits];
Delete From DimProcedures;
DBCC CHECKIDENT ('DimProcedures', RESEED, 0);
go

--Select * From StagedProcedures;
--Select * From [DimProcedures];
--go

--Insert Into [dbo].[DimProcedures] 
--([ProcedureID]
--,[ProcedureName]
--,[ProcedureDesc]
--,[ProcedureCharge]
--) Select 
--  [ProcedureID] = [ID]
-- ,[ProcedureName] = [Name]
-- ,[ProcedureDesc] = [Desc]
-- ,[ProcedureCharge] = [Charge]
--  From [Patients].[dbo].[Procedures];
--go
--Select * from [dbo].[DimProcedures];
--go

/* [dbo].[DimProcedures] */ 
If (Select Object_ID('vETLDimProcedures')) is NOT null Drop View vETLDimProcedures;
go

CREATE VIEW vETLDimProcedures AS
SELECT
	 ProcedureID = CAST(ID AS int)
	,ProcedureName = CAST(ISNULL([Name], 'Check') AS nVarchar(100))
	,ProcedureDesc = CAST(ISNULL([Desc], 'Check') AS nVarchar(1000))
	,ProcedureCharge = CAST(ISNULL([Charge], NULL) AS money)
FROM StagedProcedures;
go
-- Select * From vETLDimProcedures

If (Select Object_ID('pETLInsDimProcedures')) is NOT null Drop Procedure pETLInsDimProcedures;
go
Create Procedure pETLInsDimProcedures
AS
/************************************************************************************************
Desc: Inserts Transformed Data into [dbo].[DimProcedures]
Dev: FGomez
Date: 03/07/2024
Change Log: (When, Who, What)
************************************************************************************************/
Begin
	Declare @RC int = 0;
	Begin Try 
		-- ETL Processing Code --
			Set NoCount On;
    -- 1) In a Type 1 SCD table its easier to do a DELETE any Updated rows first...
		With ChangedProcedures
		As( Select [ProcedureID], [ProcedureName], [ProcedureDesc], [ProcedureCharge] From vETLDimProcedures
			  Except
			Select [ProcedureID], [ProcedureName], [ProcedureDesc], [ProcedureCharge] From [dbo].[DimProcedures]
		)Delete From [DWClinicReportData].[dbo].[DimProcedures]     
		  Where [ProcedureID] IN (Select [ProcedureID] From ChangedProcedures)
		;      
    -- 2) ... then add them back for as an new INSERT. This code inserts both new and updated rows! 
		With NewOrChangedProcedures
		As(	Select [ProcedureID], [ProcedureName], [ProcedureDesc], [ProcedureCharge] From vETLDimProcedures
			  Except
			Select [ProcedureID], [ProcedureName], [ProcedureDesc], [ProcedureCharge] From [dbo].[DimProcedures]
		) Insert Into [DWClinicReportData].[dbo].[DimProcedures]
      ([ProcedureID], [ProcedureName], [ProcedureDesc], [ProcedureCharge])
      Select [ProcedureID], [ProcedureName], [ProcedureDesc], [ProcedureCharge]
       From vETLDimProcedures
		    Where [ProcedureID] IN (Select [ProcedureID] From NewOrChangedProcedures)
		; 
    -- 3) For Delete, you can either delete the row, or BETTER yet Flag the row as Deleted
		With DeletedProcedures
			As( Select [ProcedureID], [ProcedureName], [ProcedureDesc], [ProcedureCharge] From [dbo].[DimProcedures]
			    Except
			    Select [ProcedureID], [ProcedureName], [ProcedureDesc], [ProcedureCharge] From vETLDimProcedures
   		)Update [DimProcedures] -- See demo below on patindex()
        Set [ProcedureName] = iif(patindex('%(Deleted)%',[ProcedureName]) > 0, [ProcedureName], [ProcedureName] + ' (Deleted)')            
		     Where [ProcedureID] IN (Select [ProcedureID] From DeletedProcedures)
	   ;
		-- ETL Processing Code --
		Print 'Success in inserting data into [dbo].[DimProcedures] ETL Process'
		Set @RC = 1;
  End Try
  Begin Catch 
		Print 'Error inserting data into [dbo].[DimProcedures] ETL Process'
		Print ERROR_MESSAGE()
		Set @RC = -1;
  End Catch
  Return @RC;
End
go

--Execute pETLInsDimProcedures
--Select * from DimProcedures;
--go 

--Delete From [dbo].[FactDoctorShifts];
--Delete From [dbo].[FactVisits];
--Delete From DimDoctors;
--DBCC CHECKIDENT ('DimPatients', RESEED, 0);
--go

-- DimShifts -----------------------------------------------------------------------------------------------------------------------------
Delete From [dbo].[FactDoctorShifts];
Delete From [dbo].[FactVisits];
Delete From DimShifts;
DBCC CHECKIDENT ('DimShifts', RESEED, 0);
go

--Select * From StagedShifts;
--Select * From [DimShifts];
--go

/* [dbo].[DimShifts] */
If (Select Object_ID('vETLDimShifts')) is NOT null Drop View vETLDimShifts;
go
Create View vETLDimShifts
AS
	Select
    [ShiftID]
   ,[ShiftStart] = Case [ShiftStart]
                    When '09:00:00' Then '09:00:00'
                    When '01:00:00' Then '13:00:00'
                    When '21:00:00' Then '21:00:00'
                    Else [ShiftStart]
                   End
   ,[ShiftEnd] = Case [ShiftEnd]
                    When '05:00:00' Then '17:00:00'
                    When '21:00:00' Then '21:00:00'
                    When '09:00:00' Then '09:00:00'
                   End
	From StagedShifts;
go

Insert Into [dbo].[DimShifts]
([ShiftID], [ShiftStart], [ShiftEnd])
Select 
[ShiftID], [ShiftStart], [ShiftEnd]
From vETLDimShifts;

If (Select Object_ID('pETLInsDimShifts')) is NOT null Drop Procedure pETLInsDimShifts;
go
Create Procedure pETLInsDimShifts
AS
/************************************************************************************************
Desc: Inserts Transformed Data into [dbo].[DimShifts]
Dev: FGomez
Date: 03/07/2024
Change Log: (When, Who, What)
************************************************************************************************/
Begin
	Declare @RC int = 0;
	Begin Try 
		-- ETL Processing Code --
			Set NoCount On;
    -- 1) In a Type 1 SCD table its easier to do a DELETE any Updated rows first...
		With ChangedShifts
		As( Select [ShiftID], [ShiftStart], [ShiftEnd] From vETLDimShifts
			  Except
			Select [ShiftID], [ShiftStart], [ShiftEnd] From [dbo].[DimShifts]
		)Delete From [DWClinicReportData].[dbo].[DimShifts]     
		  Where [ShiftID] IN (Select [ShiftID] From ChangedShifts)
		;      
    -- 2) ... then add them back for as an new INSERT. This code inserts both new and updated rows! 
		With NewOrChangedShifts
		As(	Select [ShiftID], [ShiftStart], [ShiftEnd] From vETLDimShifts
			  Except
			Select [ShiftID], [ShiftStart], [ShiftEnd] From [dbo].[DimShifts]
		) Insert Into [DWClinicReportData].[dbo].[DimShifts]
      ([ShiftID], [ShiftStart], [ShiftEnd])
      Select [ShiftID], [ShiftStart], [ShiftEnd]
       From vETLDimShifts
		    Where [ShiftID] IN (Select [ShiftID] From NewOrChangedShifts)
		; 
    -- 3) For Delete, you can either delete the row, or BETTER yet Flag the row as Deleted
		With DeletedShifts
			As( Select [ShiftID], [ShiftStart], [ShiftEnd] From [dbo].[DimShifts]
			    Except
			    Select [ShiftID], [ShiftStart], [ShiftEnd] From vETLDimShifts
   		)DELETE FROM DimShifts WHERE ShiftID IN (SELECT ShiftID FROM DeletedShifts)
		--Update [DimShifts] -- See demo below on patindex()
  --      Set [ProcedureName] = iif(patindex('%(Deleted)%',[ProcedureName]) > 0, [ProcedureName], [ProcedureName] + ' (Deleted)')            
		--     Where [ProcedureID] IN (Select [ProcedureID] From DeletedShifts)
	   ;
		-- ETL Processing Code --
		Print 'Success in inserting data into [dbo].[DimShifts] ETL Process'
		Set @RC = 1;
	End Try
    Begin Catch 
		Print 'Error inserting data into [dbo].[DimShifts] ETL Process'
		Print ERROR_MESSAGE()
		Set @RC = -1;
    End Catch
    Return @RC;
End
go

--Execute pETLInsDimShifts
--Select * from DimShifts;
--go 

--Delete From [dbo].[FactDoctorShifts];
--Delete From [dbo].[FactVisits];
--Delete From DimShifts;
--DBCC CHECKIDENT ('DimShifts', RESEED, 0);
--go

-- DimDates -----------------------------------------------------------------------------------------------------------------------------
Delete From [dbo].[FactDoctorShifts];
Delete From [dbo].[FactVisits];
Delete From [dbo].[DimDates]
DBCC CHECKIDENT ('DimDates', RESEED, 0);
go

--Select * From [DimDates];
--go


-- Now we can use the lookup table to create new dates values for the reporting database.
-- This process is FASTER AND MORE EFFICENT then creating the dates in multiple tables

/* [dbo].[DimDates] */
go
If (Select Object_ID('vDimDates')) is NOT null Drop View vDimDates;
go
Create View vDimDates
AS
Select Top 100 Percent
 [DateID]
,[FullDate]
,[FullDateName] = DateName(dw,FullDate) + ', '+ Convert(nVarchar(50), FullDate, 110)
,[MonthID] = Left(Cast(DateID as char(8)), 6)
,[MonthName] = DateName(mm,FullDate) + ' '+ Convert(nVarchar(50), Year(FullDate)) 
,[YearID] = Year(FullDate)
,[YearName] = Convert(nVarchar(50), Year(FullDate)) 
From TempDB.dbo.LookupDates
Where DateID >=20050101 and DateID <= 20111231
Order By DateID
;
go

-- DimDates has an automated Identity column, but we would like to use our own values, so...
Insert Into DimDates
([FullDate], [FullDateName], [MonthID], [MonthName], [YearID], [YearName])
Select 
 [FullDate]
,[FullDateName]
,[MonthID]
,[MonthName]
,[YearID]
,[YearName] 
From vDimDates
;

If (Select Object_ID('pETLInsDimDates')) is NOT null Drop Procedure pETLInsDimDates;
go
Create Procedure pETLInsDimDates
AS
/************************************************************************************************
Desc: Inserts Transformed Data into [dbo].[DimDates]
Dev: FGomez
Date: 03/07/2024
Change Log: (When, Who, What)
************************************************************************************************/
Begin
	Declare @RC int = 0;
	Begin Try 
		-- ETL Processing Code --
			Set NoCount On;
    -- 1) In a Type 1 SCD table its easier to do a DELETE any Updated rows first...
		With ChangedDates
		As( Select [FullDate], [FullDateName], [MonthID], [MonthName], [YearID], [YearName] From vDimDates
			  Except
			Select [FullDate], [FullDateName], [MonthID], [MonthName], [YearID], [YearName] From [dbo].[DimDates]
		)Delete From [DWClinicReportData].[dbo].[DimDates]     
		  Where [FullDate] IN (Select [FullDate] From ChangedDates)
		;      
    -- 2) ... then add them back for as an new INSERT. This code inserts both new and updated rows! 
		With NewOrChangedDates
		As(	Select [FullDate], [FullDateName], [MonthID], [MonthName], [YearID], [YearName] From vDimDates
			  Except
			Select [FullDate], [FullDateName], [MonthID], [MonthName], [YearID], [YearName] From [dbo].[DimDates]
		) Insert Into [DWClinicReportData].[dbo].[DimDates]
      ([FullDate], [FullDateName], [MonthID], [MonthName], [YearID], [YearName])
      Select [FullDate], [FullDateName], [MonthID], [MonthName], [YearID], [YearName]
       From vDimDates
		    Where [FullDate] IN (Select [FullDate] From NewOrChangedDates)
		; 
    -- 3) For Delete, you can either delete the row, or BETTER yet Flag the row as Deleted
		With DeletedDates
			As( Select [FullDate], [FullDateName], [MonthID], [MonthName], [YearID], [YearName] From [dbo].[DimDates]
			    Except
			    Select [FullDate], [FullDateName], [MonthID], [MonthName], [YearID], [YearName] From vDimDates
   		)DELETE FROM DimDates WHERE FullDate IN (SELECT FullDate FROM DeletedDates)
		--Update [DimShifts] -- See demo below on patindex()
  --      Set [ProcedureName] = iif(patindex('%(Deleted)%',[ProcedureName]) > 0, [ProcedureName], [ProcedureName] + ' (Deleted)')            
		--     Where [ProcedureID] IN (Select [ProcedureID] From DeletedShifts)
	   ;
		-- ETL Processing Code --
		Print 'Success in inserting data into [dbo].[DimDates] ETL Process'
		Set @RC = 1;
	End Try
    Begin Catch 
		Print 'Error inserting data into [dbo].[DimDates] ETL Process'
		Print ERROR_MESSAGE()
		Set @RC = -1;
    End Catch
    Return @RC;
End
go

--Execute pETLInsDimDates;
--Select * From DimDates;
--go

--Delete From [dbo].[FactDoctorShifts];
--Delete From [dbo].[FactVisits];
--Delete From [dbo].[DimDates]

/**************** Load Fact Tables ********************************************/
-- FactDoctorShifts -----------------------------------------------------------------------------------------------------------------------------
Delete From FactDoctorShifts;
go

--Select Top 2 * From StagedDoctorShifts
--Select * From FactDoctorShifts
--go

---- Getting the hours works only works if you cleaned the time data to 24 hrs
--Select *, HoursWorked = Abs(DateDiff(hh, ShiftStart, ShiftEnd)) 
-- From StagedShifts -- ORIGNINAL
--go
---- Like this!
--Select *, HoursWorked = Abs(DateDiff(hh, ShiftStart, ShiftEnd)) 
-- From DimShifts -- FIXED
--go
--Select * From StagedDoctorShifts
--go

/* [dbo].[FactDoctorShifts] */
If (Select Object_ID('vETLFactDoctorShifts')) is NOT null Drop View vETLFactDoctorShifts;
go
Create View vETLFactDoctorShifts
AS
  Select
   DoctorsShiftID = sds.[DoctorsShiftID]
  ,ShiftDateKey = dd.DateKey
  ,ClinicKey = dc.ClinicKey
  ,ShiftKey = ds.ShiftKey
  ,DoctorKey = ddr.DoctorKey
  ,ShiftStart = ds.ShiftStart -- must be normalize to a 24hr clock
  ,ShiftEnd = ds.ShiftEnd -- must be normalize to a 24hr clock 
  ,HoursWorked = Abs(DateDiff(hh, ds.ShiftStart, ds.ShiftEnd)) -- Must pull from DimShifts!
  From StagedDoctorShifts as sds
  Join [dbo].[DimDates] as dd -- Get Surrogate Key
    on sds.ShiftDate = dd.FullDate
  Join [dbo].[DimClinics] as dc -- Get Surrogate Key
    on sds.ClinicID = dc.ClinicID
  Join [dbo].[DimShifts] as ds -- Get Surrogate Key 
	on sds.ShiftID = ds.ShiftID
  Join DimDoctors as ddr
    on sds.DoctorID = ddr.DoctorID
  -- Where ds.ShiftID = 3 -- No one actually works shift 3 yet!
go

--Select * From vETLFactDoctorShifts;
--go

If (Select Object_ID('pETLInsFactDoctorShifts')) is NOT null Drop Procedure pETLInsFactDoctorShifts;
go
Create Procedure pETLInsFactDoctorShifts
AS
/************************************************************************************************
Desc: Inserts Transformed Data into [dbo].[FactDoctorShifts]
Dev: FGomez
Date: 03/11/2024
Change Log: (When, Who, What)
************************************************************************************************/
Begin
	Declare @RC int = 0;
	Begin Try 
		-- ETL Processing Code --
			Set NoCount On;	
			Merge Into FactDoctorShifts as t
			 Using vETLFactDoctorShifts as s -- For Merge to work with SCD tables, I need to insert a new row when the following is not true:
			  On t.DoctorsShiftID = s.DoctorsShiftID
			 And t.ShiftDateKey = s.ShiftDateKey
			 And t.ClinicKey = s.ClinicKey
			 And t.ShiftKey = s.ShiftKey
			 And t.DoctorKey = s.DoctorKey
			 And t.HoursWorked = s.HoursWorked			 
			 When Not Matched By Target -- At least one column value does not match add a new row:
			  Then
			   Insert (DoctorsShiftID, ShiftDateKey, ClinicKey, ShiftKey, DoctorKey, HoursWorked)
			   Values (DoctorsShiftID, ShiftDateKey, ClinicKey, ShiftKey, DoctorKey, HoursWorked)
			When Not Matched By Source
			 Then
			  Delete
			;
			Set NoCount Off;	
		-- ETL Processing Code --
		Print 'Success in inserting data into [dbo].[FactDoctorShifts] ETL Process'
		Set @RC = 1;
  End Try
  Begin Catch 
		Print 'Error inserting data into [dbo].[FactDoctorShifts] ETL Process' 
		Print ERROR_MESSAGE() 
		Set @RC = -1;
  End Catch
  Return @RC;
End
go

--Delete From StagedDoctorShifts Where DoctorsShiftID = 2;
--Select * From StagedDoctorShifts Where DoctorsShiftID = 2;

--Execute pETLInsFactDoctorShifts;
--Select * From FactDoctorShifts;
--go

--Delete From FactDoctorShifts;
--go

-- FactVisits -----------------------------------------------------------------------------------------------------------------------------
Delete From FactVisits;
go

--Select 
-- *, 
-- [Fixed Date] = Cast([Date] as date), 
-- [Fixed Clinic] = Case Clinic When 100 Then 1 When 200 Then 2 When 300 Then 3 End 
--From [Patients].dbo.Visits;
--go
--Select * From FactVisits;

-- To fix the Nulls in the data we add a row that will record a Zeroth Doctor ID
Set Identity_Insert DimDoctors On;
go
Insert Into DimDoctors 
([DoctorKey], [DoctorID], [DoctorFullName], [DoctorEmailAddress], [DoctorCity], [DoctorState], [DoctorZip])
Values
(0, 0, 'Unknown', 'Unknown', 'Unknown', 'Unknown', '00000')
Set Identity_Insert DimDoctors Off;
go
--Select * From DimDoctors;
--go

--Insert Into FactVisits
--([VisitKey], [DateKey], [ClinicKey], [PatientKey], [DoctorKey], [ProcedureKey], [ProcedureVistCharge])
--Select 
--  [VisitKey] = ID
-- ,[DateKey] = dd.DateKey
-- ,[ClinicKey] = dc.ClinicKey
-- ,[PatientKey] = dp.PatientKey
-- ,[DoctorKey] = IsNull(ddr.DoctorKey, 0)
-- ,[ProcedureKey] = dpr.[ProcedureKey]
-- ,[ProcedureVistCharge] = Sum(dpr.[ProcedureCharge])
--From [Patients].dbo.Visits as v
--Join [dbo].[DimDates] as dd -- Get Surrogate Key
--  on Cast(v.[Date] as Date) = dd.FullDate
--Join [dbo].[DimClinics] as dc -- Get Surrogate Key
--  on Case Clinic When 100 Then 1 When 200 Then 2 When 300 Then 3 End = dc.ClinicID
--Join [dbo].DimPatients as dp
--  on v.Patient = dp.PatientID
--Join DimDoctors as ddr
--  on v.Doctor = ddr.DoctorID
--Join DimProcedures as dpr
--  on v.[Procedure] = dpr.ProcedureID
--Group By
--  ID
-- ,dd.DateKey
-- ,dc.ClinicKey
-- ,dp.PatientKey
-- ,ddr.DoctorKey
-- ,dpr.[ProcedureKey];
--go

/* [dbo].[FactVisits] */
If (Select Object_ID('vETLFactVisits')) is NOT null Drop View vETLFactVisits;
go
Create View vETLFactVisits
AS
Select 
  [VisitKey] = ID
 ,[DateKey] = dd.DateKey
 ,[ClinicKey] = dc.ClinicKey
 ,[PatientKey] = dp.PatientKey
 ,[DoctorKey] = IsNull(ddr.DoctorKey, 0)
 ,[ProcedureKey] = dpr.[ProcedureKey]
 ,[ProcedureVistCharge] = Sum(dpr.[ProcedureCharge])
From [Patients].dbo.Visits as v
Join [dbo].[DimDates] as dd -- Get Surrogate Key
  on Cast(v.[Date] as Date) = dd.FullDate
Join [dbo].[DimClinics] as dc -- Get Surrogate Key
  on Case Clinic When 100 Then 1 When 200 Then 2 When 300 Then 3 End = dc.ClinicID
Join [dbo].DimPatients as dp
  on v.Patient = dp.PatientID
Join DimDoctors as ddr
  on v.Doctor = ddr.DoctorID
Join DimProcedures as dpr
  on v.[Procedure] = dpr.ProcedureID
WHERE [dp].[IsCurrent] = 1
Group By
  ID
 ,dd.DateKey
 ,dc.ClinicKey
 ,dp.PatientKey
 ,ddr.DoctorKey
 ,dpr.[ProcedureKey];
go

--Select * From vETLFactVisits;
--go

If (Select Object_ID('pETLInsFactVisits')) is NOT null Drop Procedure pETLInsFactVisits;
go
Create Procedure pETLInsFactVisits
AS
/************************************************************************************************
Desc: Inserts Transformed Data into [dbo].[FactVisits]
Dev: FGomez
Date: 03/11/2024
Change Log: (When, Who, What)
************************************************************************************************/
Begin
	Declare @RC int = 0;
	Begin Try 
		-- ETL Processing Code --
			Set NoCount On;	
			Merge Into FactVisits as t
			 Using vETLFactVisits as s -- For Merge to work with SCD tables, I need to insert a new row when the following is not true:
			  On t.VisitKey = s.VisitKey
			 And t.DateKey = s.DateKey
			 And t.ClinicKey = s.ClinicKey
			 And t.PatientKey = s.PatientKey
			 And t.DoctorKey = s.DoctorKey
			 And t.ProcedureKey = s.ProcedureKey
			 And t.ProcedureVistCharge = s.ProcedureVistCharge
			 When Not Matched By Target -- At least one column value does not match add a new row:
			  Then
			   Insert (VisitKey, DateKey, ClinicKey, PatientKey, DoctorKey, ProcedureKey, ProcedureVistCharge)
			   Values (VisitKey, DateKey, ClinicKey, PatientKey, DoctorKey, ProcedureKey, ProcedureVistCharge)
			When Not Matched By Source
			 Then
			  Delete
			;
			Set NoCount Off;	
		-- ETL Processing Code --
		Print 'Success in inserting data into [dbo].[FactVisits] ETL Process'
		Set @RC = 1;
  End Try
  Begin Catch 
		Print 'Error inserting data into [dbo].[FactVisits] ETL Process' 
		Print ERROR_MESSAGE() 
		Set @RC = -1;
  End Catch
  Return @RC;
End
go

--Execute pETLInsFactVisits;
--Select * From FactVisits;
--go

--Delete From FactVisits;
--go

/**************** TEST TABLE DATA *************************************************/
Execute pETLInsDimClinics
Execute pETLInsDimDoctors
Execute pETLInsDimPatients
Execute pETLInsDimProcedures
Execute pETLInsDimShifts
Execute pETLInsDimDates
Execute pETLInsFactDoctorShifts
Execute pETLInsFactVisits

Select * From [dbo].[DimClinics]; --3 rows
Select * From [dbo].[DimDates]; --2556 rows
Select * From [dbo].[DimDoctors]; --14 rows
Select * From [dbo].[DimPatients]; --999 rows
Select * From [dbo].[DimProcedures]; --49 rows
Select * From [dbo].[DimShifts]; --3 rows
Select * From [dbo].[FactDoctorShifts]; --10962 rows
Select * From [dbo].[FactVisits]; --40302 rows

Select * --40302 rows
From [dbo].[FactVisits] as fv
Join [dbo].[DimClinics] as dc
 On fv.ClinicKey = dc.ClinicKey
Join [dbo].[DimDates] as dd
 On fv.DateKey = dd.DateKey
Join [dbo].[DimDoctors] as ddc
 On fv.DoctorKey = ddc.DoctorKey
Join [dbo].[DimPatients] as dp
 On fv.PatientKey = dp.PatientKey
Join [dbo].[DimProcedures] as dpr
 On fv.ProcedureKey = dpr.ProcedureKey

 --TOTAL OUTPUT WINDOW ROWS = 118,542 FOR THIS SQL SCRIPT
