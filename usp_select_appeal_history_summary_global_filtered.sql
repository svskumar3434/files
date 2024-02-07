SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER OFF
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[usp_select_appeal_history_summary_global_filtered]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[usp_select_appeal_history_summary_global_filtered]
GO

CREATE Proc [dbo].[usp_select_appeal_history_summary_global_filtered] ( 
	@data_filter_definitions_id int,
	@appeal_type_id char(2) = '',
	@authorization_type_id tinyint = 1,
	@SortOrder varchar(1000)
) 
as
begin

-- DOCUMENTATION--------------------------------------------------------------------------------------------------
-- This stored procedure gets all the history records for a given data filter definition and appeal type.
-- It also checks to see if the client is set up for this definition. 
--CHANGES---------------------------------------------------------------------------------------------------------
-- 07/01/2011 fnoll  Created
-- 08/12/2011 fnoll  added label to dbname.
-- 10/20/2011 fnoll  added sort order
-- 11/10/2011 fnoll  corrected car_name definition
-- 11/16/2011 fnoll  Accomodate for language review changes.
-- 11/17/2011 fnoll  Add tracking number and change name order.
-- 07/05/2012 Fnoll  Add old appeal status
-- 07/11/2012 Fnoll  Changed size of descriptions
-- 12/06/2012 cdean  Added date_changed.
-- 04/04/2013 Fnoll  Added icon information
-- 09/07/2016 Schev  Added has_member_representative to accomodate change to usp_select_appeal_history_summary_filtered 
-- 03/27/2018 Shamkumar UMT-82 - made changes for a new icon
-- Fnoll 07-29-2020 M360ENH-2577 add queue 17 data definition - Correct db_name query
-- Shamkumar 03/15/2021 UMT-659 Add external_review_sent_date & external_review_determination_date
-- 07/19/2021 Pradeep JIRA-286 Fix on the Hrs in Q derived from date_changed
-- TDMeyer 09/20/2021 BSHBBI-1452 Appeal extensions: Added appeal extension to temp table
-- 12/14/2021 Pulakhandamr - BSHBBI-1822-Indicator in the Appeals Queues in BBI
-- 1/21/2022 nthota - BSHBBI-1923 Adding SortOrder
-- 2/27/2023 bb modified for data_filter_definitions_id = 19 BSHBBI-4536
-- 05/22/2023 BSHBBI-5269 Added new column expedited review requested
-- NG 08/25/23 Adding queues BSHBBI-5613

declare @system varchar (16)
declare @car_id int 
declare @language_review_queue tinyint 

--Create a temp table for the global items you collect.
create table #appeal_history_global_link
(db_name varchar (30),
appeal_history_id int)

--Create a temp table for the appeals records you collect
create table #appeal_history
(car_name varchar (100),
appeal_history_id int,
auth_id varchar (30),
appeal_type_id char (2),
appeal_type_description varchar (50),
appeal_contact_description varchar (50),
member_name varchar (100),
appeal_physician_name varchar (100),
proc_desc varchar (100),
appeal_received_date datetime,
appeal_response_due_date datetime,
reviewing_physician_name varchar (100),   
expedited_appeal_flag tinyint,
status_desc varchar (50),
authorization_type_id tinyint,
tracking_number varchar(17),
old_appeal_status_desc varchar (50),
date_changed datetime,
hours_in_queue int,
dob datetime,	        --Fnoll 4/4/2013
retro_flag char (1),        --Fnoll 4/4/2013
cad_program_flag tinyint,   --Fnoll 4/4/2013
expedite_flag char (1),     --Fnoll 4/4/2013
contact_type_id tinyint,    --Fnoll 4/4/2013
line_of_business char(2),   --Fnoll 4/4/2013
fax_status_id int,	        --Fnoll 4/4/2013
md_callout_status_id int,    --Fnoll 4/4/2013
has_member_representative int, --Schev 9/7/2016
has_special_timeliness_requirement int, -- Shamkumar 3/27/2018
external_review_sent_date datetime, -- Shamkumar 03/15/2021 UMT-659
external_review_determination_date datetime,
appeal_extended int,   -- TDMeyer 9/24/21  BSHBBI-1452
extend_notification_complete int,  -- TDMeyer 9/24/21  BSHBBI-1452
expedited_review_requested tinyint default(0), --- Shiva 05/22/23
dt_of_last_doc_rcvd datetime, --Pulakhandamr - 12/14/21 - BSHBBI-1822
auth_queue varchar(255),
auth_appeal_queues varchar(8000)
)

--Determine which subsystem the data filter will retrieve
set @system = (select system from niacore..data_filter_definitions (nolock) 
				where data_filter_definitions_id = @data_filter_definitions_id)

set @car_id = (select car_id from niacore..health_carrier (nolock) 
               where db_name = db_name())

set @language_review_queue = 0

if @system = 'appeals'
begin

--Override for Language Review Queue which occurs after determination and final status
if @data_filter_definitions_id = 10
	begin
		set @language_review_queue = 1
		set @data_filter_definitions_id = 4
	end
--Now lets go and pick up from the global filter, the items we are looking for. 
if (@appeal_type_id  = '' or @appeal_type_id is null) 
--We are selecting all the appeals status codes valid for that data filter.
	insert into #appeal_history_global_link 
		select db_name,appeal_history_id
		from niacore..appeal_history_global_link
		where appeal_status in (select a.assignment
								from niacore..data_filter_assignments a (nolock),
									 niacore..data_filter_definitions b (nolock)
								where a.data_filter_definitions_id = b.data_filter_definitions_id
								and b.data_filter_definitions_id = @data_filter_definitions_id  )
else
	insert into #appeal_history_global_link 
		select db_name,appeal_history_id 
		from niacore..appeal_history_global_link
		where appeal_status in (select a.assignment
								from niacore..data_filter_assignments a (nolock),
									 niacore..data_filter_definitions b (nolock)
								where a.data_filter_definitions_id = b.data_filter_definitions_id
								and b.data_filter_definitions_id = @data_filter_definitions_id  )
		and appeal_type_id = @appeal_type_id 

--This will set the queue back to the language review queue after it has picked up all the databases with final 
--determinations.
if @language_review_queue = 1
	begin
		set @data_filter_definitions_id = 10
	end

if @data_filter_definitions_id = 19
begin
	insert into #appeal_history_global_link
	(db_name, appeal_history_id)
	select hc.db_name, 0 
	from niacore..health_carrier hc with (nolock)
	join niacore..auth_queue_header aqh on hc.car_id = aqh.car_id
	where hc.date_contract_inactive is null
	and hc.date_contract_active < sysdatetime() 
	and aqh.auth_queue_code in ('uma')
	union
	select db_name, 0
	from niacore..appeal_history_global_link with (nolock)
	where appeal_status in (select a.assignment
								from niacore..data_filter_assignments a (nolock),
									 niacore..data_filter_definitions b (nolock)
								where a.data_filter_definitions_id = b.data_filter_definitions_id
								and b.data_filter_definitions_id in (2, 8, 9, 17)  )
end

-----------------------------------------------
-- Begin Cursor
-----------------------------------------------
declare	@db_name varchar(30),
        @appeal_history_id int,
		@sql_exec varchar (2000)

declare forwardcursor1 cursor for

	-- Fnoll 07-29-2020 M360ENH-2577
	select distinct db_name from #appeal_history_global_link 
	where db_name in (select db_name from niacore..client_db with (nolock) where date_inactive is null)

open forwardcursor1
fetch next from forwardcursor1 into @db_name 
while @@fetch_status = 0
begin

set @sql_exec = @db_name + 
				'..usp_select_appeal_history_summary_filtered ' 

insert into  #appeal_history
exec   @sql_exec  @data_filter_definitions_id, @appeal_type_id, @authorization_type_id, @SortOrder  


fetch next from forwardcursor1 into @db_name
end

close forwardcursor1
deallocate forwardcursor1
-----------------------------------------------
-- end of cursor
-----------------------------------------------





if @SortOrder is null or @SortOrder = ''
select @SortOrder = 'expedited_appeal_flag desc, appeal_response_due_date asc '

declare @msql varchar(max)

		set @msql =
'select 
car_name,
appeal_history_id,
auth_id,
appeal_type_id,
appeal_type_description,
appeal_contact_description,
member_name ,
appeal_physician_name,
proc_desc,
appeal_received_date,
appeal_response_due_date,
reviewing_physician_name,   
expedited_appeal_flag,
status_desc,
authorization_type_id,
tracking_number,
old_appeal_status_desc,
date_changed,	-- Pradeep 07/19/2021 JIRA-286
hours_in_queue,
dob,		    --Fnoll 4/4/2013
retro_flag,	    --Fnoll 4/4/2013
cad_program_flag,	    --Fnoll 4/4/2013
expedite_flag,	    --Fnoll 4/4/2013
contact_type_id,	    --Fnoll 4/4/2013
line_of_business,	    --Fnoll 4/4/2013
fax_status_id,	    --Fnoll 4/4/2013
md_callout_status_id,    --Fnoll 4/4/2013
has_member_representative, --Schev 9/7/2016
has_special_timeliness_requirement, -- Shamkumar 3/27/2018
external_review_sent_date, -- Shamkumar 03/15/2021 UMT-659
external_review_determination_date,
expedited_review_requested,
dt_of_last_doc_rcvd,
auth_queue,
auth_appeal_queues
from #appeal_history
order by  '+@SortOrder
EXEC(@msql)


	
end

drop table #appeal_history_global_link
drop table #appeal_history

end



GO
GRANT EXECUTE ON [dbo].[usp_select_appeal_history_summary_global_filtered] TO [db_execallsp]
GO
GRANT EXECUTE ON [dbo].[usp_select_appeal_history_summary_global_filtered] TO [informa_admin]
GO
GRANT EXECUTE ON [dbo].[usp_select_appeal_history_summary_global_filtered] TO [informa_users]
GO
GRANT EXECUTE ON [dbo].[usp_select_appeal_history_summary_global_filtered] TO [sf_it]
GO
