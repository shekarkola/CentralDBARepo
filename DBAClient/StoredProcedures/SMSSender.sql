USE [DBAClient]
GO
/****** Object:  StoredProcedure [dbo].[SMSSender]    Script Date: 10/10/2024 12:04:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*----------------------------------------------------------------------------------------------------------------------------------
-- Author:			Shekar Kola
-- Create date:		2019-10-08
-- Description:		To Send SMS using EDS gateway from database engine, 
					this procedure can be used from different application databases, 
					therefore, it's good practice to pass appropriate application name into parametre @AppName for logging purpose

Versions:
	2023-10-19
		- SMS Gateway API Endpiont changed by vendor
	
	2019-10-08
		- Initial Version
-------------------------------------------------------------------------------------------------------------------------------------*/

ALTER   PROCEDURE [dbo].[SMSSender] 
		@AppName nvarchar (25),
		@PhoneNumbers nvarchar (1000), 
		@Message nvarchar (1000), 
		@SenderID nvarchar(25),
		@LogRetention int = 365
		
AS
BEGIN

	SET NOCOUNT ON;

	IF (SELECT OBJECT_ID ('SMSLog')) IS NULL 
	BEGIN 
		CREATE TABLE [dbo].[SMSLog](
			[SessionID] [bigint] NOT NULL,
			[AppName] [varchar](25) NULL,
			[SessionUser] [varchar](50) NULL,
			[LogDatetime] [datetime] NULL,
			[ParamSenderCode] [varchar](100) NULL,
			[ParamMobileNumbers] [varchar](1000) NULL,
			[ParamMessageText] [varchar](1000) NULL,
			[SMSStatus] [varchar](25) NULL,
			[StausText] [varchar](500) NULL,
			[ResponseText] [varchar](500) NULL,
			[CallDetails] [varchar](2000) NULL
		)
		ALTER TABLE [dbo].[SMSLog] ADD  DEFAULT (getdate()) FOR [LogDatetime];

		CREATE CLUSTERED INDEX [IX_SMSLog] ON [dbo].[SMSLog] ([SessionID]);
	END
			DECLARE @SessionID bigint;
			DECLARE @SessionUser nvarchar (128);
			DECLARE @authHeader nvarchar(64);
			DECLARE @contentType nvarchar(64);
			DECLARE @postData nvarchar(2000);
			DECLARE @responseText nvarchar(500);
			DECLARE @responseXML nvarchar(2000);
			DECLARE @ret INT;
			DECLARE @status nvarchar(32);
			DECLARE @statusText nvarchar(32);
			DECLARE @token INT;
			DECLARE @url nvarchar(MAX);
			DECLARE @APIurl nvarchar(MAX);

			---SET @authHeader = 'BASIC 0123456789ABCDEF0123456789ABCDEF';
			SET @contentType = 'application/x-www-form-urlencoded';
			SELECT @postData = 'destination=' + @PhoneNumbers + '&text='+ @Message + '';
				PRINT @postData;
			SET @url = 'https://portal.smshub.live/API/SendSMS?username=POMHOLDING&apiId=8m6Gjjmg&json=True&source='+@SenderID+'&';
			SET @APIurl = @url + @postData;
				PRINT @APIurl;

			-- Open the connection.
			EXEC @ret = sp_OACreate 'MSXML2.ServerXMLHTTP', @token OUT;
			IF @ret <> 0 RAISERROR('Unable to open HTTP connection.', 10, 1);

			Else 
			BEGIN
				select @SessionID = CAST( (convert (nvarchar(20), GETDATE(), 112 ) + 
									REPLACE( REPLACE( (convert (nvarchar(14), SYSDATETIME(), 114 )), ':', ''), '.', '' ) ) 
									AS bigint);
				select @SessionUser = ISNULL(SUSER_SNAME(), CURRENT_USER);
				-- Send the request.
				EXEC @ret = sp_OAMethod @token, 'open', NULL, 'POST', @APIurl, 'false';
				--EXEC @ret = sp_OAMethod @token, 'setRequestHeader', NULL, 'Authentication', @authHeader;
				--EXEC @ret = sp_OAMethod @token, 'setRequestHeader', NULL, 'Content-type', @contentType;
				EXEC @ret = sp_OAMethod @token, 'send', NULL, @APIurl;

				-- Handle the response.
				EXEC @ret = sp_OAGetProperty @token, 'status', @status OUT;
				EXEC @ret = sp_OAGetProperty @token, 'statusText', @statusText OUT;
				EXEC @ret = sp_OAGetProperty @token, 'responseText', @responseText OUT;

			-- Response logging...
			Insert	into SMSLog (SessionID, AppName, [SessionUser], ParamSenderCode, ParamMobileNumbers, ParamMessageText, [StausText], [ResponseText], CallDetails) 
					values (@SessionID, @AppName, @SessionUser, @SenderID, @PhoneNumbers, @Message, @statusText, @ResponseText, @APIurl);

			PRINT 'Status: ' + @status + ' (' + @statusText + ')';
			PRINT 'Response text: ' + @responseText;

			END
			

			-- Close the connection.
			EXEC @ret = sp_OADestroy @token;
			IF @ret <> 0 RAISERROR('Unable to close HTTP connection.', 10, 1);


		---- Log Clean-up 
		DELETE FROM SMSLog WHERE LogDatetime <= DATEADD(DAY, -365, GETDATE());
END
