/************************************************************************************************
Title: ETL Final Project: DWClinicReportData Load
Desc: This file contains ETL code for BI ETL Final.
Dev: RRoot
Date: 03/07/2020
Change Log: (When, Who, What)
			3/11/2020,RRoot, Separated Pre-load, Load, and Post-load ETL code for clarity.
			3/11/2020,RRoot, Added ETL insert code for all tables.

IMPORTANT: You must create a Linked Server for this script to work.

USE [master]
Go
EXEC master.dbo.sp_addlinkedserver 
  @server = N'CONTINUUMSQL.WESTUS2.CLOUDAPP.AZURE.COM'
, @srvproduct=N'SQL Server'

EXEC master.dbo.sp_addlinkedsrvlogin 
  @rmtsrvname=N'CONTINUUMSQL.WESTUS2.CLOUDAPP.AZURE.COM'
, @useself=N'False'
, @locallogin=NULL
, @rmtuser=N'BICert'
, @rmtpassword='BICert'
Go

 -- AND

USE [master]
GO
EXEC master.dbo.sp_addlinkedserver 
  @server = N'IS-ROOT01.ISCHOOL.UW.EDU\BI'
, @srvproduct=N'SQL Server'

GO
USE [master]
GO
EXEC master.dbo.sp_addlinkedsrvlogin 
  @rmtsrvname = N'IS-ROOT01.ISCHOOL.UW.EDU\BI'
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
 From [CONTINUUMSQL.WESTUS2.CLOUDAPP.AZURE.COM].[DoctorsSchedules].[dbo].[Clinics]; 
go
Select * From StagedClinics;
go
If (Object_ID('StagedDoctors') is not null) Drop Table StagedDoctors;
go
Select *
 Into StagedDoctors
 From [CONTINUUMSQL.WESTUS2.CLOUDAPP.AZURE.COM].[DoctorsSchedules].[dbo].[Doctors]; 
go
Select * From StagedDoctors;
go
If (Object_ID('StagedDoctorShifts') is not null) Drop Table StagedDoctorShifts;
go
Select *
 Into StagedDoctorShifts
 From [CONTINUUMSQL.WESTUS2.CLOUDAPP.AZURE.COM].[DoctorsSchedules].[dbo].[DoctorShifts]; 
go
Select * From StagedDoctorShifts;

go
If (Object_ID('StagedShifts') is not null) Drop Table StagedShifts;
go
Select *
 Into StagedShifts
 From [CONTINUUMSQL.WESTUS2.CLOUDAPP.AZURE.COM].[DoctorsSchedules].[dbo].[Shifts]; 
go
Select * From StagedShifts;
go


/**************** Load Dimension Tables ********************************************/
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
	--From [CONTINUUMSQL.WESTUS2.CLOUDAPP.AZURE.COM].[DoctorsSchedules].[dbo].[Clinics]; 
go
-- Select * From vETLDimClinics

If (Select Object_ID('pETLInsDimClinics')) is NOT null Drop Procedure pETLInsDimClinics;
go
Create Procedure pETLInsDimClinics
AS
/************************************************************************************************
Desc: Inserts Transformed Data into [dbo].[DimClinics]
Dev: RRoot
Date: 03/07/2030
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
		Print 'Error inserting data into  [dbo].[DimClinics] ETL Process'
		Print ERROR_MESSAGE()
		Set @RC = -1;
  End Catch
  Return @RC;
End
go

Execute pETLInsDimClinics;
Select * From DimClinics;
go


-- Now, we test that the code works!
-- 1) Clear the table and reset the Identity Spec.
Delete From [dbo].[FactDoctorShifts];
Delete From [dbo].[FactVisits];
Delete From [dbo].[DimClinics];
DBCC CHECKIDENT ('dbo.DimClinics', RESEED, 0); 
go

Select * From DimClinics;
go

-- 2) Test the initial Fill
Execute pETLInsDimClinics;
Select * From DimClinics;
go

-- 3) Test inserting data
Insert Into StagedClinics 
([ClinicID], [ClinicName], [Address], [City], [State], [Zip])
Values(4, 'TestIns', 'TestIns', 'TestIns','TestIns', '98000');
Select * From StagedClinics;
go

Execute pETLInsDimClinics;
Select * From DimClinics;
go

-- 4) Test updating data
Update StagedClinics
 Set [ClinicName] = 'TestUPDATE' Where ClinicName = 'TestIns';
Select * From StagedClinics;
go

Execute pETLInsDimClinics;
Select * From DimClinics;
go

-- 4) Test deleting data
Delete From StagedClinics 
  Where ClinicName = 'TestUPDATE';   
Select * From StagedClinics;
go

Execute pETLInsDimClinics;
Select * From DimClinics;
go

-- DimDoctors --
Delete From [dbo].[FactDoctorShifts];
Delete From [dbo].[FactVisits];
Delete From DimDoctors;
DBCC CHECKIDENT ('DimPatients', RESEED, 0);
go

Select * From StagedDoctors;
Select * From [DimDoctors];
go

Insert Into [dbo].[DimDoctors]
([DoctorID], [DoctorFullName], [DoctorEmailAddress], [DoctorCity], [DoctorState], [DoctorZip])
Select 
 [DoctorID]
,[DoctorFullName] = [FirstName] + ' ' + [LastName]
,[DoctorEmailAddress] = [EmailAddress]
,[DoctorCity] =  LTrim([City])
,[DoctorState] = Replace(Replace([State],' ', ''),'Redmond','')
,[DoctorZip] = [Zip]
From StagedDoctors;
-- Could have done this -> From [CONTINUUMSQL.WESTUS2.CLOUDAPP.AZURE.COM].[DoctorsSchedules].[dbo].[Doctors];
go

Select * from DimDoctors;
Print 'TODO: create a view / stored procedure for this!';
go 

/* [dbo].[DimPatients] */
Select * 
Into StagedPatients
From [IS-ROOT01.ISCHOOL.UW.EDU\BI].[Patients].[dbo].[Patients];
go

Delete From [dbo].[FactDoctorShifts];
Delete From [dbo].[FactVisits];
Delete From DimPatients;
DBCC CHECKIDENT ('DimPatients', RESEED, 0);
go

Insert Into [dbo].[DimPatients] 
([PatientID]
,[PatientFullName]
,[PatientCity]
,[PatientState]
,[PatientZipCode]
,[StartDate]
,[EndDate]
,[IsCurrent]
) Select 
   [PatientID] = [ID]
  ,[PatientFullName] = [FName] + ' ' + [LName]
  ,[PatientCity] = [City]
  ,[PatientState] = [State]
  ,[PatientZipCode] = [ZipCode]
  ,[StartDate] = GetDate()
  ,[EndDate] = Null
  ,[IsCurrent]= 1
  From StagedPatients
  Order By ID;
go

Select * from [dbo].[DimPatients];
Print 'TODO: create a view / stored procedure for this!'
go 

/* [dbo].[DimProcedures] */ 
Delete From [dbo].[FactDoctorShifts];
Delete From [dbo].[FactVisits];
Delete From DimProcedures;
DBCC CHECKIDENT ('DimProcedures', RESEED, 0);
go

Select * 
Into StagedPrcedures
From [IS-ROOT01.ISCHOOL.UW.EDU\BI].[Patients].[dbo].[Patients];
go

Insert Into [dbo].[DimProcedures] 
([ProcedureID]
,[ProcedureName]
,[ProcedureDesc]
,[ProcedureCharge]
) Select 
  [ProcedureID] = [ID]
 ,[ProcedureName] = [Name]
 ,[ProcedureDesc] = [Desc]
 ,[ProcedureCharge] = [Charge]
  From [Patients].[dbo].[Procedures];
go

Select * from [dbo].[DimProcedures];
Print 'TODO: create a view / stored procedure for this!'
go 

/* [dbo].[DimShifts] */
Delete From [dbo].[FactDoctorShifts];
Delete From [dbo].[FactVisits];
Delete From DimShifts;
DBCC CHECKIDENT ('DimShifts', RESEED, 0);
go

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
go

Select * From [DWClinicReportData].[dbo].[DimShifts];
Print 'TODO: create a view / stored procedure for this!';
go 

/* [dbo].[DimDates] */
Delete From [dbo].[FactDoctorShifts];
Delete From [dbo].[FactVisits];
Delete From [dbo].[DimDates]
-- We are going to create dates a bit different this time, by using a LOOKUP/DOMAIN table
If Exists(Select Name From TempDB.Sys.Tables Where Name = 'Dates') Drop Table TempDB.dbo.Dates;
go

Set nocount ON;
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

-- Now we can use the lookup table to create new dates values for the reporting database.
-- This process is FASTER AND MORE EFFICENT then creating the dates in multiple tables
go
If (Select Object_ID('vDimDates')) is NOT null Drop View vDimDates;
go
Create View vDimDates
AS
Select Top 100 Percent
 [DateID]
,[FullDate]
,[DateName] = DateName(dw,FullDate) + ', '+ Convert(nVarchar(50), FullDate, 110)
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
,[DateName]
,[MonthID]
,[MonthName]
,[YearID]
,[YearName] 
From vDimDates
;

Select * From DimDates;
Print 'TODO: create a view / stored procedure for this!'
go
Print '---------------------------------------------------------------------'

/**************** Load Fact Tables ********************************************/
/* [dbo].[FactDoctorShifts] */
Delete From FactDoctorShifts;
go

Select Top 2 * From StagedDoctorShifts
Select * From FactDoctorShifts
go

-- Getting the hours works only works if you cleaned the time data to 24 hrs
Select *, HoursWorked = Abs(DateDiff(hh, ShiftStart, ShiftEnd)) 
 From StagedShifts -- ORIGNINAL
go
-- Like this!
Select *, HoursWorked = Abs(DateDiff(hh, ShiftStart, ShiftEnd)) 
 From DimShifts -- FIXED
go
Select * From StagedDoctorShifts
go

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
Select * From vETLFactDoctorShifts;
go

If (Select Object_ID('pETLInsFactDoctorShifts')) is NOT null Drop Procedure pETLInsFactDoctorShifts;
go
Create Procedure pETLInsFactDoctorShifts
AS
/************************************************************************************************
Desc: Inserts Transformed Data into [dbo].[FactDoctorShifts]
Dev: RRoot
Date: 03/11/2030
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
		Print 'Success in inserting data into [dbo].[DimDoctorShiftss] ETL Process'
		Set @RC = 1;
  End Try
  Begin Catch 
		Print 'Error inserting data into [dbo].[DimDoctorShifts] ETL Process' 
		Print ERROR_MESSAGE() 
		Set @RC = -1;
  End Catch
  Return @RC;
End
go
--Delete From StagedDoctorShifts Where DoctorsShiftID = 2;
--Select * From StagedDoctorShifts Where DoctorsShiftID = 2;
Execute pETLInsFactDoctorShifts;
Select * From FactDoctorShifts;
go

/* [dbo].[FactVisits] */
Select 
 *, 
 [Fixed Date] = Cast([Date] as date), 
 [Fixed Clinic] = Case Clinic When 100 Then 1 When 200 Then 2 When 300 Then 3 End 
From [Patients].dbo.Visits;
go
Select * From FactVisits;

-- To fix the Nulls in the data we add a row that will record a Zeroth Doctor ID
Set Identity_Insert DimDoctors On;
go
Insert Into DimDoctors 
([DoctorKey], [DoctorID], [DoctorFullName], [DoctorEmailAddress], [DoctorCity], [DoctorState], [DoctorZip])
Values
(0, 0, 'Unknown', 'Unknown', 'Unknown', 'Unknown', '00000')
Set Identity_Insert DimDoctors Off;
go
Select * From DimDoctors;
go

Insert Into FactVisits
([VisitKey], [DateKey], [ClinicKey], [PatientKey], [DoctorKey], [ProcedureKey], [ProcedureVistCharge])
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
Group By
  ID
 ,dd.DateKey
 ,dc.ClinicKey
 ,dp.PatientKey
 ,ddr.DoctorKey
 ,dpr.[ProcedureKey];
go
Print 'TODO: create a view / stored procedure for this!'
go
Print '---------------------------------------------------------------------'

/**************** TEST TABLE DATA *************************************************/
Select * From [dbo].[DimClinics];
Select * From [dbo].[DimDates];
Select * From [dbo].[DimDoctors];
Select * From [dbo].[DimPatients];
Select * From [dbo].[DimProcedures];
Select * From [dbo].[DimShifts];
Select * From [dbo].[FactDoctorShifts];
Select * From [dbo].[FactVisits];

Select * 
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

