/***************************************************************************
ETL Final Project: Staging Tables ETL for Patient's Database; Bulk Insert from CSV Files to Staging Tables
Dev: FGomez
Date:03/03/2024
Desc: This is a Staging Database ETL for the Patients database ETL processing issues
ChangeLog: (Who, When, What) 
    FGomez, 3/3/2024, Developed for staging tables to push Patients csv data to our Patient's database
*****************************************************************************************/

/****** RESET DATABASES [Patients] and [DoctorsSchedules] AS NEEDED **********************/
USE MASTER
GO
ALTER DATABASE [Patients] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
RESTORE DATABASE [Patients] 
 FROM DISK = N'C:/_BISolutions/Databases/Patients.bak' 
 WITH RECOVERY, REPLACE;
ALTER DATABASE [Patients] SET MULTI_USER;
go

ALTER DATABASE [DoctorsSchedules] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
RESTORE DATABASE [DoctorsSchedules] 
 FROM DISK = N'C:/_BISolutions/Databases/DoctorsSchedules.bak' 
 WITH RECOVERY, REPLACE;
ALTER DATABASE [Patients] SET MULTI_USER;
go
/****** RESET DATABASES [Patients] and [DoctorsSchedules] AS NEEDED **********************/

----------------------------------------------------------------------------

USE TempDB;
GO
CREATE OR ALTER PROC pETLCreateStagingTables AS
/***************************************************************************
Desc: Creates STaging TAbles for File Import
Dev: FGomez
Date:03/03/2024
ChangeLog: (Who, When, What) 
    FGomez, 3/3/2024, Developed for staging tables for each CSV file in our ClinicDailyData.zip folder
*****************************************************************************************/
Begin
	Declare @RC int = 0;
	Begin Try
		If (SELECT Object_ID('StagingBellevueNewPatients')) IS NOT NULL
		DROP TABLE StagingBellevueNewPatients;
		CREATE TABLE StagingBellevueNewPatients
		([FName] nvarchar(100)
		,[LName] nvarchar(100)
		,[Email] nvarchar(100)
		,[Address] nvarchar(100)
		,[City] nvarchar(100)
		,[State] nvarchar(100)
		,[ZipCode] nvarchar(100)
		);

		If (SELECT Object_ID('StagingBellevueVisits')) IS NOT NULL
		DROP TABLE StagingBellevueVisits;
		CREATE TABLE StagingBellevueVisits
		([Time] nvarchar(100)
		,[Patient] nvarchar(100)
		--,[Clinic] nvarchar(100) -- This data is missing in the file!
		,[Doctor] nvarchar(100)
		,[Procedure] nvarchar(100)
		,[Charge] nvarchar(100)
		);

		If (SELECT Object_ID('StagingKirklandNewPatients')) IS NOT NULL
		DROP TABLE StagingKirklandNewPatients;
		CREATE TABLE StagingKirklandNewPatients
		([FName] nvarchar(100)
		,[LName] nvarchar(100)
		,[Email] nvarchar(100)
		,[Address] nvarchar(100)
		,[City] nvarchar(100)
		,[State] nvarchar(100)
		,[ZipCode] nvarchar(100)
		);

		If (SELECT Object_ID('StagingKirklandVisits')) IS NOT NULL
		DROP TABLE StagingKirklandVisits;
		CREATE TABLE StagingKirklandVisits
		([Time] nvarchar(100)
		,[Patient] nvarchar(100)
		,[Clinic] nvarchar(100)
		,[Doctor] nvarchar(100)
		,[Procedure] nvarchar(100)
		,[Charge] nvarchar(100)
		);

		If (SELECT Object_ID('StagingRedmondNewPatients')) IS NOT NULL
		DROP TABLE StagingRedmondNewPatients;
		CREATE TABLE StagingRedmondNewPatients
		([FName] nvarchar(100)
		,[LName] nvarchar(100)
		,[Email] nvarchar(100)
		,[Address] nvarchar(100)
		,[City] nvarchar(100)
		,[State] nvarchar(100)
		,[ZipCode] nvarchar(100)
		);

		If (SELECT Object_ID('StagingRedmondVisits')) IS NOT NULL
		DROP TABLE StagingRedmondVisits;
		CREATE TABLE StagingRedmondVisits
		([Time] nvarchar(100)
		,[Clinic] nvarchar(100) -- Different Column Order Issue!
		,[Patient] nvarchar(100) -- Different Column Order Issue!
		,[Doctor] nvarchar(100)
		,[Procedure] nvarchar(100)
		,[Charge] nvarchar(100)
		);
		Set @RC = 1;
		End Try
		Begin Catch
		Print 'Error Creating Staging Tables for File Import'
		Print ERROR_MESSAGE()
		Set @RC = -1;
		End Catch
		Return @RC;
	End
	GO
EXEC pETLCreateStagingTables;
SELECT [name], [crdate] FROM sysobjects WHERE NAME LIKE 'Staging%'
SELECT * FROM StagingBellevueNewPatients
SELECT * FROM StagingBellevueVisits
SELECT * FROM StagingKirklandNewPatients
SELECT * FROM StagingKirklandVisits
SELECT * FROM StagingRedmondNewPatients
SELECT * FROM StagingRedmondVisits
GO


-- Insert Dat From Files --
-------------------------------------------------------------------------------------------------
CREATE OR ALTER PROC pETLImportFileDataToStagingTables AS
/************************************************************************************************
Desc: Imports File Data To Staging Tables
Dev: FGomez
Date: 03/10/2024
Change Log: (When, Who, What)
	FGomez, 3/3/2024, Developed for performing BULK INSERT into our staging tables for each CSV file in our ClinicDailyData.zip folder
************************************************************************************************/
Begin
	Declare @RC int = 0;
	Begin Try
		Bulk Insert StagingBellevueNewPatients
		From 'C:\_BISolutions\ClinicDailyData\Bellevue\20100102NewPatients.csv'
			With
			(DATAFILETYPE = 'char'
			,FORMAT = 'CSV'
			,ROWTERMINATOR = '\n'
			,FIRSTROW = 2
			)

		Bulk Insert StagingBellevueVisits
		From 'C:\_BISolutions\ClinicDailyData\Bellevue\20100102Visits.csv'
			With
			(DATAFILETYPE = 'char'
			,FORMAT = 'CSV'
			,ROWTERMINATOR = '\n'
			,FIRSTROW = 2
			);

		Bulk Insert StagingKirklandNewPatients
		From 'C:\_BISolutions\ClinicDailyData\Kirkland\20100102NewPatients.csv'
			With
			(DATAFILETYPE = 'char'
			,FORMAT = 'CSV'
			,ROWTERMINATOR = '\n'
			,FIRSTROW = 2
			)

		Bulk Insert StagingKirklandVisits
		From 'C:\_BISolutions\ClinicDailyData\Kirkland\20100102Visits.csv'
			With
			(DATAFILETYPE = 'char'
			,FORMAT = 'CSV'
			,ROWTERMINATOR = '\n'
			,FIRSTROW = 2
			);

		Bulk Insert StagingRedmondNewPatients
		From 'C:\_BISolutions\ClinicDailyData\Redmond\20100102NewPatients.csv'
			With
			(DATAFILETYPE = 'char'
			,FORMAT = 'CSV'
			,ROWTERMINATOR = '\n'
			,FIRSTROW = 2
			)

		Bulk Insert StagingRedmondVisits
		From 'C:\_BISolutions\ClinicDailyData\Redmond\20100102Visits.csv'
			With
			(DATAFILETYPE = 'char'
			,FORMAT = 'CSV'
			,ROWTERMINATOR = '\n'
			,FIRSTROW = 2
			);

		Set @RC = 1;
	End Try
	Begin Catch
		Print 'Error Importing File Data to Staging Tables'
		Print ERROR_MESSAGE()
		Set @RC = -1;
	End Catch
	Return @RC;
End
GO
Exec pETLImportFileDataToStagingTables;
GO


-- Review the Data --
---------------------------------------------------------------------------------------
SELECT * FROM StagingBellevueNewPatients
SELECT * FROM StagingBellevueVisits
SELECT * FROM StagingKirklandNewPatients
SELECT * FROM StagingKirklandVisits
SELECT * FROM StagingRedmondNewPatients
SELECT * FROM StagingRedmondVisits

-- Transforms the new Data --
----------------------------------------------------------------------------------------
GO
CREATE OR ALTER PROC pETLTransformNewPatientsandVisitsData AS
/***************************************************************************************
Desc: Transforms Data in Staging TAbles
Dev: FGomez
Date: 03/03/2024
Change Log: (When, Who, What)
	FGomez, 3/3/2024, Developed for transforming our data in our staging tables after performing BULK INSERT into our staging tables for each CSV file in our ClinicDailyData.zip folder
****************************************************************************************/
Begin
	Declare @RC int = 0;
	Begin Try
		Update [tempdb].[dbo].[StagingBellevueNewPatients] -- Bellevue
		Set [Email] = [Email] + ' (Invalid) '
		Where (PatIndex('%@%', [Email]) = 0) OR (PatIndex('%.com%', [Email]) = 0);

		Update [tempdb].[dbo].[StagingKirklandNewPatients] -- Kirkland
		Set [Email] = [Email] + ' (Invalid) '
		Where (PatIndex('%@%', [Email]) = 0) OR (PatIndex('%.com%', [Email]) = 0);

		Update [tempdb].[dbo].[StagingRedmondNewPatients] -- Redmond
		Set [Email] = [Email] + ' (Invalid) '
		Where (PatIndex('%@%', [Email]) = 0) OR (PatIndex('%.com%', [Email]) = 0);
		Set @RC = 1;
	End Try
	Begin Catch
		Print 'Error Importing File Data to Staging Tables'
		Print ERROR_MESSAGE()
		Set @RC = -1;
	End Catch
	Return @RC;
End
GO
Exec pETLTransformNewPatientsandVisitsData;
GO


-- Review the Data --
---------------------------------------------------------------------------------------
SELECT * FROM StagingBellevueNewPatients
SELECT * FROM StagingKirklandNewPatients
SELECT * FROM StagingRedmondNewPatients
GO

-- Combine the New Patients Data --
----------------------------------------------------------------------------------------
CREATE OR ALTER PROC pETLSelectNewPatientsData AS -- This could also be a view or function
/**********************************************************************************
Desc: Selects New Patients data from staging tables, adds a data, combines the results for all visits.
Dev: FGomez
Date: 03/03/2024
Change Log: (When, Who, What)
	Felipe Gomez, 3/3/2024, Developed for selecting New Patients data and merging our data using UNION after transforming our data in our staging tables after performing BULK INSERT into our staging tables for each CSV file in our ClinicDailyData.zip folder
************************************************************************************/
Begin
	Declare @RC int = 0;
	Begin Try
		Select
		 [Fname]
		,[LName]
		,[Email]
		,[Address]
		,[City]
		,[State]
		,[ZipCode]
		FROM StagingBellevueNewPatients
		UNION -- Combine Bellevue and Redmond data
		Select
		 [Fname]
		,[LName]
		,[Email]
		,[Address]
		,[City]
		,[State]
		,[ZipCode]
		FROM StagingRedmondNewPatients
		UNION -- now add Kirkland data
		Select
		 [Fname]
		,[LName]
		,[Email]
		,[Address]
		,[City]
		,[State]
		,[ZipCode]
		FROM StagingKirklandNewPatients
		EXCEPT -- Subtract what is already in the Visits table
		Select
		 [Fname]
		,[LName]
		,[Email]
		,[Address]
		,[City]
		,[State]
		,[ZipCode]
		FROM [Patients].[dbo].[Patients];
		Set @RC = 1;
	End Try
	Begin Catch
		Rollback Tran;
		Print 'Error Selecting Data from Staging Tables into Visits'
		Print ERROR_MESSAGE()
		Set @RC = -1;
	End Catch
	Return @RC;
END
GO
Exec pETLSelectNewPatientsData;  -- If you get Zero rows the data has already been imported!
GO


-- Insert the New Patients Data --
-----------------------------------------------------------------------------------
CREATE OR ALTER PROC pETLInsertNewPatientsData AS
/**********************************************************************************
Desc: Selects New Patients data from staging tables, adds a data, combines the results for all visits.
Dev: FGomez
Date: 03/03/2024
Change Log: (When, Who, What)
	Felipe Gomez, 3/3/2024, Developed for inserting our New Patients data after selecting and merging our data using UNION after transforming our data in our staging tables after performing BULK INSERT into our staging tables for each CSV file in our ClinicDailyData.zip folder
************************************************************************************/
Begin
	Declare @RC int = 0;
	Begin Try
	Begin Tran;
		Insert Into [Patients].[dbo].[Patients]
			([FName], [LName], [Email], [Address], [City], [State], [ZipCode])
		Exec pETLSelectNewPatientsData --using my select sproc
		Commit Tran;
		Set @RC = 1;
	End Try
	Begin Catch
	Rollback Tran;
		Print 'Error Inserting Data from Staging Tables into New Patients'
		Print ERROR_MESSAGE()
		Set @RC = -1;
	End Catch
	Return @RC;
End
GO
Exec pETLInsertNewPatientsData
GO

-- Combine the new Visits Data --
----------------------------------------------------------------------------------------
CREATE OR ALTER PROC pETLSelectVisitsData -- This could also be a view or function
(@Date date)
AS
/**********************************************************************************
Desc: Selects Visits data from staging tables, adds a data, combines the results for all visits.
Dev: FGomez
Date: 03/03/2024
Change Log: (When, Who, What)
	Felipe Gomez, 3/3/2024, Developed for selecting and merging our Visits data using UNION after transforming our data in our staging tables after performing BULK INSERT into our staging tables for each CSV file in our ClinicDailyData.zip folder
************************************************************************************/
Begin
	Declare @RC int = 0;
	Begin Try
		Select
		 [Date] =  CAST(@Date AS datetime) + Cast([Time] AS datetime)
		,[Clinic] = 100 -- Adding missing Clinic ID
		,[Patient]
		,[Doctor]
		,[Procedure]
		,[Charge]
		FROM StagingBellevueVisits
		UNION -- Combine Bellevue and Redmond data
		Select
		 [Date] = CAST(@Date AS datetime) + CAST([Time] AS datetime)
		,[Clinic] = [Clinic] * 100
		,[Patient]
		,[Doctor]
		,[Procedure]
		,[Charge]
		FROM StagingRedmondVisits
		UNION -- now add Kirkland data
		Select
		 [Date] = CAST(@Date AS datetime) + CAST([Time] AS datetime)
		,[Clinic] = [Clinic] * 100
		,[Patient]
		,[Doctor]
		,[Procedure]
		,[Charge]
		FROM StagingKirklandVisits
		EXCEPT -- Subtract what is already in the Visits table
		Select
		 [Date]
		,[Clinic]
		,[Patient]
		,[Doctor]
		,[Procedure]
		,[Charge]
		FROM [Patients].[dbo].[Visits]
		ORDER BY [Date], [Clinic];
		Set @RC = 1;
	End Try
	Begin Catch
		Rollback Tran;
		Print 'Error Selecting Data from Staging Tables into Visits'
		Print ERROR_MESSAGE()
		Set @RC = -1;
	End Catch
	Return @RC;
END
GO
Exec pETLSelectVisitsData @Date = '20100102';  -- If you get Zero rowS the data has already been imported!
GO


-- Insert the Visits Data with Date --
-----------------------------------------------------------------------------------
CREATE OR ALTER PROC pETLInsertVisitsData
(@Date date) AS
/**********************************************************************************
Desc: Inserts Visits data from staging tables, adds the data, combines the results for all visits.
Dev: FGomez
Date: 03/03/2024
Change Log: (When, Who, What)
	Felipe Gomez, 3/3/2024, Developed for inserting our Visits data after selecting and merging our data using UNION after transforming our data in our staging tables after performing BULK INSERT into our staging tables for each CSV file in our ClinicDailyData.zip folder
************************************************************************************/
Begin
	Declare @RC int = 0;
	Begin Try
	Begin Tran;
		Insert Into [Patients].[dbo].[Visits]
			([Date], [Clinic], [Patient], [Doctor], [Procedure], [Charge])
		Exec pETLSelectVisitsData @Date = @Date --using my select sproc
		Commit Tran;
		Set @RC = 1;
	End Try
	Begin Catch
	Rollback Tran;
		Print 'Error Inserting Data from Staging Tables into Visits'
		Print ERROR_MESSAGE()
		Set @RC = -1;
	End Catch
	Return @RC;
End
GO
Exec pETLInsertVisitsData @Date = '20100102'
GO

-- Test Code --
USE [master]
GO
-- reset the database
ALTER DATABASE [Patients] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
RESTORE DATABASE [PatientS]
	FROM DISK = N'C:/_BISolutions/Databases/Patients.bak'
	WITH RECOVERY, REPLACE;
ALTER DATABASE [Patients] SET MULTI_USER;
GO
Select Count(*) FROM [Patients].[dbo].[Visits] -- 40150 rows
Select Count(*) FROM [Patients].[dbo].[Patients] -- 999 rows
GO
USE tempdb;
go
Exec pETLCreateStagingTables
go
Exec pETLImportFileDataToStagingTables;
go
Exec pETLTransformNewPatientsandVisitsData;
go
Exec pETLInsertNewPatientsData;
go
Exec pETLInsertVisitsData @Date = '20100102'
go
Select Count(*) FROM [Patients].[dbo].[Visits] -- 40304 rows
Select Count(*) FROM [Patients].[dbo].[Patients] -- 1005 rows
go


-- Create Scheduled Daily Job for SSIS Package using SQL Server Agent --
-----------------------------------------------------------------------------------
USE [tempdb]
GO
CREATE OR ALTER PROC pETLJob AS
/**********************************************************************************
Desc: Schedules Daily ETL Job run to import NewPatients and Visits CSV files data into Patients Database
Dev: FGomez
Date: 03/03/2024
Change Log: (When, Who, What)
	Felipe Gomez, 3/3/2024, Developed to create a scheduled job run for our ETL process for importing NewPatients and Visits data from our CSV files into our Patients Database
************************************************************************************/
Begin
/****** Object:  Job [ETLFinalDailyScheduleRun]    Script Date: 3/10/2024 7:13:45 PM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [Database Maintenance]    Script Date: 3/10/2024 7:13:45 PM ******/
IF Exists (SELECT * FROM msdb.dbo.sysschedules WHERE name = 'Daily ETLFinal SSIS Package Run')
	EXEC msdb.dbo.sp_delete_job @job_id=N'48898b70-6fd6-4be2-9df9-ad555fb726e4', @delete_unused_schedule=1
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Database Maintenance' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Database Maintenance'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'ETLFinalDailyScheduleRun', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'SSIS Package Run for Final', 
		@category_name=N'Database Maintenance', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Daily Schedule Run]    Script Date: 3/10/2024 7:13:45 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Daily Schedule Run', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'SSIS', 
		@command=N'/FILE "\"C:\_BISolutions\UWETLFinal_FelipeGomez\ETLFinalSSISPackages\ETLFilesToDatabases.dtsx\"" /CHECKPOINTING OFF /REPORTING E', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Daily ETLFinal SSIS Package Run', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20240310, 
		@active_end_date=99991231, 
		@active_start_time=10000, 
		@active_end_time=235959, 
		@schedule_uid=N'9b774154-0269-4d0d-9f29-8f35284fa218'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
End
GO


