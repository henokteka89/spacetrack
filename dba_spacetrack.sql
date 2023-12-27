--the stored proc

USE [DBAUtil]
GO

/****** Object:  StoredProcedure [dbo].[DBA_DBFileSizeCheck]    Script Date: 12/27/2023 9:12:37 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[DBA_DBFileSizeCheck]  --- Default Threshold Page alert 4096MB and Email alert 8192MB 

AS

SET NOCOUNT ON
DECLARE @Mail INT
DECLARE @Page INT
DECLARE @sysFree INT
DECLARE @syscnt INT
DECLARE @body VARCHAR(Max)
DECLARE @title VARCHAR(250) 
DECLARE @query_Email VARCHAR(250)
DECLARE @query_Pager VARCHAR(250)
DECLARE @xml NVARCHAR(MAX)
DECLARE @PageAlertMB INT = 4096, @EmailAlertMB INT= 8192
Declare @emailTo VARCHAR(100) = 'sqldba@

.com', @EmailandPagerTo  VARCHAR(100) = 'henokteka89@gmail.com'
-- 

----- Every Monday this part captures the file size and stored in tbl_FileSizeTracking table for database growth planning purpose

IF DATEname(WEEKDAY,GETDATE()) = 'Monday' AND DATEPART(hh, GETDATE()) IN (8,9,10)
BEGIN
	INSERT INTO DBAUtil..tbl_FileSizeTracking
	SELECT DB_name(database_id) AS Databasename
	     , CAST([name] AS varchar(20)) AS nameofFile
	     , CAST(physical_name AS varchar(100)) AS PhysicalFile
	     ,  type_desc AS FileType
	     , ((CAST(size AS BIGINT) * 8)/1024) AS FileSize
	     , MaxFileSize = CASE WHEN max_size = -1 OR max_size = 268435456 THEN 'UNLIMITED'
	                          WHEN max_size = 0 THEN 'NO_GROWTH' 
	                          WHEN max_size <> -1 OR max_size <> 0 THEN CAST(((CAST(max_size AS BIGINT) * 8) / 1024) AS varchar(15))
	                          ELSE 'Unknown'
	                       END
	     , SpaceRemainingMB = CASE WHEN max_size = -1 OR max_size = 268435456 THEN 'UNLIMITED'
	                               WHEN max_size <> -1 OR max_size = 268435456 THEN CAST(((CAST(max_size - size AS BIGINT) * 8) / 1024) AS varchar(10))
	                               ELSE 'Unknown'
	                          END
	     , Growth = CASE WHEN growth = 0 THEN 'FIXED_SIZE'
	                     WHEN growth > 0 THEN STR((growth * 8)/1024)
	                     ELSE 'Unknown'
	                END
	     , GrowthType = CASE WHEN is_percent_growth = 1 THEN 'PERCENTAGE'
	                         WHEN is_percent_growth = 0 THEN 'MBs'
	                         ELSE 'Unknown'
	                    END
	     , GETDATE() AS chkdate
	FROM master.sys.master_files
	WHERE state = 0
	  AND type_desc IN ('LOG', 'ROWS')
	  AND database_id NOT IN (1,2,3,4)
	ORDER BY database_id, file_id

	DELETE FROM DBAUtil..tbl_FileSizeTracking WHERE chkdate < GETDATE()- 366 
END

---SQLDBA team to be notified for any Data/Log file in fixed sized- MAXSIZE = LIMITED
---What action DBA need to take when they got email/Page about this Alert : Whenever we get this alert - 
---We check what files trigger the alert and understand why   if we find the alert is generated due to MaxSize set limited , unless there is known/specific reason we changed the Maxsize to UNLIMITED   this will definitely address the alert and that is what we need 
---(addressing non-standard file configuration)
---This PROC check as per the schedule and page or mail for space related with Data/Log file with fixed sized

------ Grab all database file related info 

IF OBJECT_ID('tempdb..#AlldatabaseFileinfo') IS NOT NULL DROP TABLE #AlldatabaseFileinfo;

CREATE TABLE #AlldatabaseFileinfo ([Databasename] [nvarchar](128) NULL,[Filename] [sysname] NOT NULL,[PhysicalFileLocation] [nvarchar](260) NULL,[FileType] [varchar](4) NULL,	
[CurrentSizeinMB] [int] NULL,[AvailSpace] [int] NULL,[Growth] [varchar](22) NULL,[max_size] [int] NOT NULL,[TotalReserverdSpaceMB] [int] NULL,[FileGroupname] [nvarchar](128) NULL, CheckedTime varchar(30) DEFAULT CONVERT(varchar(30), getdate(), 120)
) 

INSERT INTO #AlldatabaseFileinfo ([Databasename] ,[Filename],[PhysicalFileLocation] ,[FileType],[CurrentSizeinMB],[AvailSpace] ,[Growth] ,[max_size] ,[TotalReserverdSpaceMB] ,[FileGroupname],CheckedTime)
EXEC sp_MSforeachdb
'use [?];

SELECT Databasename = DB_name(),   
	sdf.[name] AS [Filename],    
	physical_name AS [Physicalname],    
	CASE sdf.type   WHEN 0 THEN ''Data'' WHEN 1 THEN ''Log''  END ''FileType'',   
	CASE ceiling([size]/128) WHEN 0 THEN 1 ELSE ceiling([size]/128) END ''CurrentSizeinMB'',   
	CASE ceiling([size]/128) WHEN 0 THEN (1 - CAST(FILEPROPERTY(sdf.[name], ''SpaceUsed'') as int) /128)ELSE (([size]/128) - CAST(FILEPROPERTY(sdf.[name], ''SpaceUsed'') as int) /128) END ''AvailSpace'',   
	CASE [is_percent_growth]   WHEN 1 THEN CAST(growth AS varchar(20)) + ''%'' ELSE CAST(growth*8/1024 AS varchar(20)) + ''MB''END ''Growth'',  
	max_size , 
	CAST(((CAST(max_size AS BIGINT) * 8)/1024) AS int) as [TotalReserverdSpaceMB],
	--CASE WHEN max_size = -1 OR max_size = 268435456 THEN ''UNLIMITED'' WHEN max_size = 0 THEN ''NO_GROWTH'' 
	--WHEN max_size <> -1 OR max_size <> 0 THEN CAST(((CAST(max_size AS BIGINT) * 8) / 1024) AS varchar(15))ELSE ''Unknown'' end as Max_Filesize_Decription_MB,
	Case When fg.name is Null Then ''LogFile'' Else fg.name end FileGroupname,--Transaction log files are never part of any filegroups
	Getdate() as ''CheckedTime''
FROM sys.database_files sdf  
left JOIN sys.filegroups fg
ON sdf.data_space_id=fg.data_space_id
where DB_NAME() not in ( select name from sys.databases 
where is_read_only=1 OR is_in_standby=1)
'

--Select * from #AlldatabaseFileinfo

----Get Lists of Data/Log file with fixed sized. 
----In case of multiple data files with limited size, 
IF OBJECT_ID('tempdb..#SpaceRemaining_perFileMB') IS NOT NULL DROP TABLE #SpaceRemaining_perFileMB;
IF OBJECT_ID('tempdb..#SpaceRemaining_AcrossFilesMB') IS NOT NULL DROP TABLE #SpaceRemaining_AcrossFilesMB;
IF OBJECT_ID('tempdb..#NumberFilePerFileGroup') IS NOT NULL DROP TABLE #NumberFilePerFileGroup;

--- Calculate NumberFilePerFileGroup to exclued from alerting -alert should be fired only if there are single files per file group
Select Databasename, FileGroupname,count(*) as NumberFilePerFileGroup
Into #NumberFilePerFileGroup
from #AlldatabaseFileinfo
Group by Databasename, FileGroupname
--order by Databasename
--- Calculate SpaceRemaining per File and used for reporting in the pager/email
Select al.Databasename, al.FileGroupname, [PhysicalFileLocation],[TotalReserverdSpaceMB] - [CurrentSizeinMB] as SpaceRemaining_perFileMB
Into #SpaceRemaining_perFileMB
FROM #AlldatabaseFileinfo al
Join #NumberFilePerFileGroup nu
on nu.Databasename = al.Databasename and nu.FileGroupname =al.FileGroupname
And max_size <> -1 and max_size <> 268435456 and max_size <> 0 and nu.NumberFilePerFileGroup < 2 ---, -1 = File will grow until the disk is full. 268435456 = Log file will grow to a maximum size of 2 TB.0 = No growth is allowed.

----Select * from #SpaceRemaining_perFileMB
----- Calculate SpaceRemainingMB per DataBase,FileType, FileGroupname (if there is multiple filegroup) and  used for alert triggering 
--Select Databasename, FileType, FileGroupname,(SUM([TotalReserverdSpaceMB]) -SUM([CurrentSizeinMB])) as SpaceRemainingMB
--Into #SpaceRemaining_AcrossFilesMB
--FROM #AlldatabaseFileinfo 
--where max_size <> -1 and max_size <> 268435456 and max_size <> 0 
--Group by Databasename,FileType,FileGroupname

--select* from #SpaceRemaining_AcrossFilesMB
Declare @SQLDBArunbook nvarchar(max)
set @SQLDBArunbook =N'<H3><FONT SIZE="2" FACE="Tahoma"> '+'For possible actions, please refer SQLDBA RUNBOOK : https://abc/SQL+-+Data+Log+file+in+fixed-sized+reached+low+space' + ' </FONT></H3>'
Declare @env_code  CHAR(3)
IF @@SERVERNAME LIKE 'GZ%'
BEGIN
SET @env_code= SUBSTRING(CONVERT(varchar(100),SERVERPROPERTY('machinename')),10,3)
END
ELSE 
BEGIN 
SET @env_code='NP1'
END
SELECT @Mail = count (*) FROM #SpaceRemaining_perFileMB WHERE SpaceRemaining_perFileMB <= @EmailAlertMB and SpaceRemaining_perFileMB >= @PageAlertMB
--SELECT @Mail = COUNT(*) FROM #SpaceRemaining_perFileMB WHERE SpaceRemaining_perFileMB  < @EmailAlertMB 

IF @Mail > 0
	BEGIN
		SELECT @title ='SQLQC Alert (Warning) '+@env_code+' - SQL Email Alert Insufficient space across all files: Data/Log file in fixed sized reached Low Space..' +@@SERVERname
	    SET @xml = CAST(( SELECT adfi.[Databasename] AS 'td','',[Filename] AS 'td','', adfi.[PhysicalFileLocation] AS 'td','', [CurrentSizeinMB] AS 'td','',srpf.[SpaceRemaining_perFileMB] AS 'td',''
		                ,[TotalReserverdSpaceMB] AS 'td','',adfi.[FileType] AS 'td','',[CheckedTime] AS 'td',''
		FROM #AlldatabaseFileinfo adfi
		Join #SpaceRemaining_perFileMB srpf
		on srpf.Databasename = adfi.Databasename and srpf.PhysicalFileLocation =adfi.PhysicalFileLocation
		WHERE SpaceRemaining_perFileMB  <=  CONVERT(VARCHAR(10),@EmailAlertMB) and SpaceRemaining_perFileMB >= CONVERT(VARCHAR(10),@PageAlertMB)
		FOR XML PATH('tr'), ELEMENTS ) AS NVARCHAR(MAX))

		SET @body ='<html><body><H3>Email Alert Info: Data/Log file in fixed sized</H3>
		<table border = 1> 
		<tr>
		<th> Databasename </th> <th> Filename </th> <th> PhysicalFileLocation </th> <th> CurrentSizeinMB </th> <th> SpaceRemaining_perFileMB </th> <th> TotalReserverdSpaceMB </th> <th> FileType </th> <th> CheckedTime </th></tr>'    

        SET @body = @body + @xml 
		+'</table></body></html>'
		+ @SQLDBArunbook

		EXEC msdb.dbo.sp_send_dbmail
		@profile_name = 'HenokEmailProfile',
		@recipients = @emailTo,
		@body = @body,
        @body_format ='HTML',
        @subject = @title;
		--@query =  @query_Email;

	EXEC xp_logevent 50001, @title, 'Warning';  		

	END

SELECT @Page = count (*) FROM #SpaceRemaining_perFileMB WHERE SpaceRemaining_perFileMB  < @PageAlertMB 
--SELECT @Page = COUNT(*) FROM ##SpaceRemaining_perFileMB WHERE SpaceRemaining_perFileMB  < @PageAlertMB 
	
    IF @Page > 0
		BEGIN
			SELECT @title ='SQLQC Alert (Critical) '+@env_code+' - SQL Page Alert Insufficient space across all files: Data/Log file in fixed sized reached Low Space..' +@@SERVERname
			SET @xml = CAST(( SELECT adfi.[Databasename] AS 'td','',[Filename] AS 'td','', adfi.[PhysicalFileLocation] AS 'td','', [CurrentSizeinMB] AS 'td','',srpf.[SpaceRemaining_perFileMB] AS 'td',''
							,[TotalReserverdSpaceMB] AS 'td','',adfi.[FileType] AS 'td','',[CheckedTime] AS 'td',''
			FROM #AlldatabaseFileinfo adfi
			Join #SpaceRemaining_perFileMB srpf
			on srpf.Databasename = adfi.Databasename and srpf.PhysicalFileLocation =adfi.PhysicalFileLocation
			WHERE SpaceRemaining_perFileMB  <  CONVERT(VARCHAR(10),@PageAlertMB)
			FOR XML PATH('tr'), ELEMENTS ) AS NVARCHAR(MAX))

			SET @body ='<html><body><H3>Page Alert Info: Data/Log file in fixed sized</H3>
			<table border = 1> 
			<tr>
			<th> Databasename </th> <th> Filename </th> <th> PhysicalFileLocation </th> <th> CurrentSizeinMB </th> <th> SpaceRemaining_perFileMB </th> <th> TotalReserverdSpaceMB </th> <th> FileType </th> <th> CheckedTime </th></tr>'    

			SET @body = @body + @xml 
			 +'</table></body></html>'
			 + @SQLDBArunbook

		    EXEC msdb.dbo.sp_send_dbmail
			@profile_name = 'HenokEmailProfile',
			@recipients = @EmailandPagerTo,
			@body = @body,
            @body_format ='HTML',
            @subject = @title;
			--@query =  @query_Pager;

	EXEC xp_logevent 50001, @title, 'ERROR';  	
		END


GO




--the job

USE [msdb]
GO

/****** Object:  Job [DBA Space_Tracking]    Script Date: 12/27/2023 9:11:32 AM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 12/27/2023 9:11:32 AM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'DBA Space_Tracking', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Step 1 Drive Space check-- threshold default 5 GB for page 25 GB for email.                                                                                         Step 2 File Growth check -- threshold default  4 GB for page  8 GB for email.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa', 
		@notify_email_operator_name=N'DBAHenok', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Drive Space Check]    Script Date: 12/27/2023 9:11:33 AM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Drive Space Check', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXEC [DBAUtil].[dbo].[DBA_DriveSpaceCheck]', 
		@database_name=N'DBAUtil', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [File Growth Check]    Script Date: 12/27/2023 9:11:33 AM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'File Growth Check', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXEC [DBAUtil].[dbo].[DBA_DBFileSizeCheck]

', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'FreeSpace', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=127, 
		@freq_subday_type=8, 
		@freq_subday_interval=4, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20230730, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959, 
		@schedule_uid=N'10517bad-c157-4e7d-b9c2-9c6c500ddc44'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO


