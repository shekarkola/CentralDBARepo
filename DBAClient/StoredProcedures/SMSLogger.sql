USE [DBAClient]
GO
/****** Object:  StoredProcedure [dbo].[SMSLogger]    Script Date: 10/10/2024 12:04:33 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:			Shekar Kola
-- Create date:		2019-11-13
-- Modified date:	2019-11-26
-- Description:		SMS notifications Log 
-- =============================================
ALTER PROCEDURE [dbo].[SMSLogger]  
		@SessionID bigint,
		@AppName varchar (25),
		@ParamMobileNumbers varchar (1000),
		@ParamSenderCode varchar (50),
		@ParamMessageText varchar (1000),
		@SessionUser varchar (100), 
		@StatusCode varchar (50),
		@StausText varchar (100), 
		@ResponseText varchar (500),
		@CallDetails varchar(2000)
		
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
	BEGIN
		Insert into SMSLog (SessionID, AppName, [SessionUser], ParamSenderCode, ParamMobileNumbers, ParamMessageText, [StausText], [ResponseText], CallDetails) 
					values (@SessionID, @AppName, @SessionUser, @ParamSenderCode, @ParamMobileNumbers, @ParamMessageText, @StausText, @ResponseText, @CallDetails);
	END
END

