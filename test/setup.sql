IF EXISTS(select * from sysobjects where id = object_id(N'[dbo].[customer]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
BEGIN
 drop table [dbo].[customer]
END
GO

CREATE TABLE [dbo].[customer](
	[idcustomer] [int] NOT NULL,
	[name] [varchar](100) NULL,
	[age] [int] NULL,
	[birth] [datetime] NULL,
	[surname] [varchar](100) NULL,
	[stamp] [datetime] NULL,
	random [int] NULL,
	curr decimal(19,2) NULL
 CONSTRAINT [PK_customer] PRIMARY KEY CLUSTERED ([idcustomer] ASC ) ON [PRIMARY]
) ON [PRIMARY]

GO
declare @i int
set @i=1
select RAND(100)
while (@i<500) BEGIN
insert into customer(idcustomer,name,age,birth,surname,stamp,random,curr) values(
			 @i,
			 'name'+convert(varchar(10),@i)
			,10+@i,
			{ts '2010-09-24 12:27:38.030'},
			'surname_'++convert(varchar(10),@i*2+100000),
			getdate(),
			RAND()*1000,
			RAND()*10000
		)
set @i=@i+1
end
GO

IF EXISTS(select * from sysobjects where id = object_id(N'[dbo].[seller]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
BEGIN
 drop table [dbo].[seller]
END
GO
CREATE TABLE [dbo].[seller](
	[idseller] [int] NOT NULL,
	[name] [varchar](100) NULL,
	[age] [int] NULL,
	[birth] [datetime] NULL,
	[surname] [varchar](100) NULL,
	[stamp] [datetime] NULL,
	random [int] NULL,
	curr decimal(19,2) NULL,
	cf varchar(200)
 CONSTRAINT [PK_seller] PRIMARY KEY CLUSTERED ([idseller] ASC ) ON [PRIMARY]
) ON [PRIMARY]
GO

declare @i int
set @i=1
select RAND(1000)
while (@i<60) BEGIN
insert into seller (idseller,name,age,birth,surname,stamp,random,curr,cf) values(
			 @i,
			 'name'+convert(varchar(10),@i)
			,10+@i,
			{ts '2010-09-24 12:27:38.030'},
			'surname_'++convert(varchar(10),@i*2+100000),
			getdate(),
			RAND()*1000,
			RAND()*10000,
			convert(varchar(20),RAND()*100000)
		)
set @i=@i+1
end

GO



IF EXISTS(select * from sysobjects where id = object_id(N'[dbo].[sellerkind]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
BEGIN
 drop table [dbo].[sellerkind]
END
GO
CREATE TABLE [dbo].[sellerkind](
	[idsellerkind] [int] NOT NULL,
	[name] [varchar](100) NULL,
	rnd [int] NULL,
 CONSTRAINT [PK_sellerkind] PRIMARY KEY CLUSTERED ([idsellerkind] ASC ) ON [PRIMARY]
) ON [PRIMARY]
GO

declare @i int
set @i=0
select RAND(1000)
while (@i<20) BEGIN
insert into sellerkind (idsellerkind,name,rnd) values(
			 @i*30,
			 'name'+convert(varchar(10),@i*30),
			RAND()*1000
		)
set @i=@i+1
end

GO

IF EXISTS(select * from sysobjects where id = object_id(N'[dbo].[customerkind]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
BEGIN
 drop table [dbo].[customerkind]
END
GO
CREATE TABLE [dbo].[customerkind](
	[idcustomerkind] [int] NOT NULL,
	[name] [varchar](100) NULL,
	rnd [int] NULL,
 CONSTRAINT [PK_customerkind] PRIMARY KEY CLUSTERED ([idcustomerkind] ASC ) ON [PRIMARY]
) ON [PRIMARY]
GO

declare @i int
set @i=0
select RAND(1000)
while (@i<40) BEGIN
insert into customerkind (idcustomerkind,name,rnd) values(
			 @i*3,
			 'name'+convert(varchar(10),@i*3),
			RAND()*1000
		)
set @i=@i+1
end

GO



if exists (select * from dbo.sysobjects where id = object_id(N'[testSP2]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [testSP2]
GO

 CREATE PROCEDURE testSP2
         @esercizio int,   @meseinizio int,   @mess varchar(200),   @defparam decimal(19,2) =  2
         AS
         BEGIN
         select 'aa' as colA, 'bb' as colB, 12 as colC , @esercizio as original_esercizio,
         replace(@mess,'a','z') as newmess,   @defparam*2 as newparam
         END

GO
if exists (select * from dbo.sysobjects where id = object_id(N'[testSP1]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [testSP1]
GO

CREATE PROCEDURE [dbo].[testSP1]
	@esercizio int,
	@meseinizio int,
	@mesefine int out,
	@mess varchar(200),
	@defparam decimal(19,2) =  2
AS
BEGIN
	set @mesefine= 12
	select 'a' as colA, 'b' as colB, 12 as colC , @esercizio as original_esercizio, 
		replace(@mess,'a','z') as newmess,
		@defparam*2 as newparam
END

GO

if exists (select * from dbo.sysobjects where id = object_id(N'[testSP3]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [testSP3]
GO
CREATE  PROCEDURE [dbo].[testSP3]
	@esercizio int = 0
AS
BEGIN
	select top 100 * from customer
	select top 100 * from seller
	select top 10 * from customerkind as c2
	select top 10 * from sellerkind as s2
END


GO

