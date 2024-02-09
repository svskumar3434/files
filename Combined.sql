
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER OFF
GO
if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[usp_select_appeal_history_summary_filtered]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[usp_select_appeal_history_summary_filtered]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER OFF
GO

create Proc [dbo].[usp_select_appeal_history_summary_filtered] ( 
	@data_filter_definitions_id int,
	@appeal_type_id char(2) = '',
	@authorization_type_id tinyint = 1,
	@SortOrder varchar(1000)
) 

As
Begin

/************************************************************************************************************************************************
-- DOCUMENTATION--------------------------------------------------------------------------------------------------
-- This stored procedure gets all the history records for a given data filter definition and appeal type.
-- It also checks to see if the client is set up for this definition. 
--CHANGES---------------------------------------------------------------------------------------------------------
-- 08/13/2011 fnoll  created.
-- 10/20/2011 fnoll  updated sort order.
-- 11/10/2011 fnoll corrected car_name definition
-- 11/17/2011 fnoll add tracking number change order of names.
-- Fnoll 02/20/2012 use niacore..appeal_status_codes instead of niacore..auth_status_codes
-- Fnoll 06/27/2012 Permit all authorization types.
-- Fnoll 07/05/2012 Add last status
-- cdean 12/06/2012 Added date_changed.
-- Fnoll 04/04/2013 Added code for icons
-- Fnoll 04/10/2013 Corrected Authorization_type_id double entry
-- Fnoll 05/22/2013 Return Nulls for last 3 column retrivals. Done to not change BBI - while reducing response time.
-- Fnoll 01/27/2014 modifying for new data filter definition 16 - Validation of recons/rereviews/reopens
--VJ changes for tkt#199715
-- NG 08/29/17 hide auths for the carriers terminated for specific auth type id.Jira:M360ENH-811
-- Shamkumar 3/27/2018 UMT-82 - made changes for a new icon
-- Fnoll 07-29-2020 M360ENH-2577 add queue 17 data definition
-- Fnoll 08/03/2020 M360ENH-2577 Added global/car begin date for queue 17
-- Shamkumar 03/15/2021 UMT-659 Add external_review_sent_date & external_review_determination_date
--VJ 04/01/2021 changes TBLAZERS-195
--VJ 05/05 adding clinical appeal status to projectID 577
--VJ 05/10 adding Unknown/undetermined appeal type to projectID 578
--VJ 05/11 changes to remove duplicates
--VJ 05/18 changes to remove duplicates when the old appeal_status is same on the Notification Queue
--VJ 05/19 changes to route standard Appeals to Notification Queue
-- TDMeyer 09/20/2021 BSHBBI-1452 Appeal extensions: adding to notification queue
-- 12/14/2021 Pulakhandamr - BSHBBI-1822-Indicator in the Appeals Queues in BBI
-- 1/21/2022 nthota - BSHBBI-1923 Adding SortOrder
--VJ 01/25/2022 BSHBBI-2200 changes to return results on Appeal Notification Queue
--VJ 02/01/2022 BSHBBI-2200 changes to return correct results on Appeal Notification Queue change the way we check for project_id
--thotan BSHBBI-3430 to move Expedited Appeal Review not granted appeals to “Appeals Notifications Queue” for member and provider appeal first level 
--Thotan 9/15/2022 Rolling back the changes for BSHBBI-3430 Expedited Appeal Notification for Expedited not granted 
--Thotan 09/20/2022 BSHBBI-3430 Enabling the rolled back changes for Expedited Appeal Notification for Expedited not granted
--Thotan 09/21/2022 BSHBBI-3440 to move "Appeal Notification Queue - Expedited Change" to “Appeals Notifications Queue” for member and provider appeal first level separately
--Thotan 09/21/2022 BSHBBI-3441 to move "Appeal Notification Queue - Standard Retro Change" to “Appeals Notifications Queue” for member and provider appeal first level separately 
--Thotan BSHBBI-4174 To only route expedited appeals to Appeal Notification Queue when appeal is completed.
--thotan 12/29/2022  BSHBBI-4332 To fix the time-out issue in Prod, replacing OR clause with INSERT statement
--thotan BSHBBI-4357 to move "Appeal Notification Queue - Expedited Appeal Review not Granted Change for Member appeal and Member Contact Type" to “Appeals Notifications Queue” for member appeal first level.
--BSHBBI-4345: SG Dawn confirmed that cases go to “Coordinator Recon Denial Review” only if Project 161 & 216 is active.
-- 2/27/2023 bb modified for data_filter_definitions_id = 19 BSHBBI-4536
--Shiva 05/08/2023 Added new parameter expedited_review_requested
-- NG 08/25/23 Adding queues BSHBBI-5613
-- Ng 09/08/23 modifying appeal queues logic BSHBBI-5977 as appeal queues were not showing correct data
**************IMPORTANT IMPORTANT     When making changes please look in SP usp_select_appeal_history_queues if that also need any changes***************************************

-- NG 09/14/23 removing 17 definition id when it is UM admin review/ICR appeals queue (19) is sent as appeal notification has a more complex logic than just looking at data defintion id 
************************************************************************************************************************************************/

declare @system varchar (16)  
declare @car_id int   
declare @car_name varchar (100)  
declare @queue_begin_date datetime  
declare @appeal_queue char(3)
declare @cnt as integer,@counter as integer,@appeal_auth_id as varchar(17)
 
create table #appeal_history_summary_filtered
( 
	car_name varchar(100),
	appeal_history_id int,
	auth_id varchar(15),
	appeal_type_id char(2),
	appeal_type_description varchar(50),
	appeal_contact_description varchar(50),
	member_name varchar(4000),
	appeal_physician_name varchar(4000),
	proc_desc varchar(100),
	appeal_received_date datetime,
	appeal_response_due_date datetime,
	reviewing_physician_name varchar(4000),
	expedited_appeal_flag tinyint,
	status_desc varchar(50),
	authorization_type_id tinyint,
	tracking_number varchar(17),
	old_appeal_status_desc varchar(100),--
	date_changed datetime,
	hours_in_queue int,
	dob datetime,
	retro_flag char(1),
	cad_program_flag char(1),
	expedite_flag char(1),
	contact_type_id tinyint,
	line_of_business varchar(15),
	fax_status_id int,
	md_callout_status_id int,
	has_member_representative tinyint,
	has_special_timeliness_requirement tinyint,
	external_review_sent_date datetime,
	external_review_determination_date datetime,
	appeal_extended tinyint,
	extend_notification_complete tinyint,
	expedited_review_requested tinyint default(0)
)

create table #auth_date_of_last_doc_rcvd
( auth_id varchar(15),
  dt_of_last_doc_rcvd datetime
)


--Determine which subsystem the data filter will retrieve  
select @system =  system,
@appeal_queue = filter_code
from niacore..data_filter_definitions (nolock)   
where data_filter_definitions_id = @data_filter_definitions_id
  
select @car_id = car_id,  
  @car_name = car_name  
from niacore..health_carrier (nolock)   
               where db_name = db_name()  
                 
if @system = 'appeals'  
begin  
  if @data_filter_definitions_id = 10
	begin
		set @language_review_queue = 1
		set @data_filter_definitions_id = 4
	end
	if @language_review_queue = 1
	begin
		set @data_filter_definitions_id = 10
	end
--Appeal Notifications Queue  
if @data_filter_definitions_id = 17   
begin  
  
	set @queue_begin_date = (select date_active from niacore..car_projects with (nolock) where project_id = 544 and car_id = @car_id and date_inactive is null)  
	if @queue_begin_date is null  
		set @queue_begin_date = (select date_active from niacore..global_projects with (nolock) where project_id = 544 and date_inactive is null)  
  

	if (@appeal_type_id  = '' or @appeal_type_id is null)   
	begin  
	insert into #appeal_history_global_link 
		select db_name,appeal_history_id
		from niacore..appeal_history_global_link
		where appeal_status in (select a.assignment
								from niacore..data_filter_assignments a (nolock),
									 niacore..data_filter_definitions b (nolock)
								where a.data_filter_definitions_id = b.data_filter_definitions_id
								and b.data_filter_definitions_id = @data_filter_definitions_id  )

		insert into #appeal_history_summary_filtered
		select Distinct @car_name car_name,  
		a.appeal_history_id,  
		a.auth_id,  
		a.appeal_type_id,  
		b.appeal_type_description,  
		c.appeal_contact_description,  
		dbo.initcap(e.lname + ', ' + e.fname) member_name,  
		dbo.initcap(g.lname + ', ' + g.fname) appeal_physician_name,   
		d.proc_desc,  
		a.appeal_received_date,  
		a.appeal_response_due_date,  
		dbo.initcap(f.lname + ', ' + f.fname) reviewing_physician_name,     
		a.expedited_appeal_flag,  
		h.appeal_status_description status_desc,  
		a.authorization_type_id,  
		d.tracking_number,  
		'' old_appeal_status_desc,  
		dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id) date_changed, 
		datediff(hour,dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id),getdate()) hours_in_queue,
		e.dob,   --4/04/2013 Fn  
		d.retro_flag,  --4/04/2013 Fn  
		d.cad_program_flag,  --4/04/2013 Fn  
		d.expedite_flag,  --4/04/2013 Fn  
		d.contact_type_id,  --4/04/2013 Fn  
		null line_of_business,     --5/22/2013 Fn  
		null fax_status_id,        --5/22/2013 Fn  
		null md_callout_status_id,  --5/22/2013 Fn  
		case when exists (select 1 from member_consenter (nolock) where member_id = e.member_id) then 1  
				else 0  
				end as has_member_representative,  
		dbo.uf_get_project(e.plan_id, 373) has_special_timeliness_requirement, --03/27/18 UMT-82  
		a.external_review_sent_date,  
		a.external_review_determination_date,  
		a.appeal_extended,  
			a.extend_notification_complete,
			a.expedited_review_requested
		from appeal_history a (nolock)  
		inner join niacore..appeal_types b (nolock) on a.appeal_type_id = b.appeal_type_id  
		inner join niacore..appeal_contacts c (nolock) on a.appeal_contact_id = c.appeal_contact_id  
		inner join authorizations d (nolock) on a.auth_id = d.auth_id  
		inner join members e (nolock) on d.member_id = e.member_id  
		left outer join niacore..is_users f (nolock) on a.reviewing_physician_id = f.is_user_id   
		inner join physicians g (nolock) on d.phys_id = g.phys_id  
		inner join niacore..appeal_status_codes h (nolock) on a.appeal_status = h.appeal_status  
		inner join appeal_status_change i on a.appeal_history_id = i.appeal_history_id     --Fnoll 7/05/2012   
		inner join niacore..appeal_status_codes j (nolock) on i.old_appeal_status = j.appeal_status         --Fnoll 7/05/2012   
		inner join niacore..appeal_info k (nolock) on a.appeal_info_id = k.appeal_info_id   -- Fnoll 07-29-2020 M360ENH-2577  
		where a.appeal_status in (select a.assignment  
			from niacore..data_filter_assignments a (nolock) ,  
				niacore..data_filter_definitions b (nolock) ,  
				niacore..data_filter_clients c (nolock)   
			where a.data_filter_definitions_id = b.data_filter_definitions_id  
			and b.data_filter_definitions_id = @data_filter_definitions_id  
			and b.data_filter_definitions_id = c.data_filter_definition_id   
			and c.car_id = @car_id)  
			--VJ changes BSHBBI-804 to move appeals to “Appeals Notifications Queue”   
						and i.isfinal = 1                   
							and    not exists (select 'true' from niacore..vw_auth_type_terminated_plans   
																			where plan_id = e.plan_id  
																			and authorization_type_id = a.authorization_type_id)  
							and (a.appeal_notification_complete is null or a.appeal_notification_complete = 0)   
							and (a.expedited_appeal_flag = 1 or k.appeal_notification_queue_all = 1)  
							and a.created_date >= @queue_begin_date  
							and a.appeal_status in ('ax','ay','az')    --2021/07/15  
		--VJ changes TBLAZERS-195 to move expedited appeals to “Appeals Notifications Queue”   
		--Thotan 09/21/2022 BSHBBI-3440 to move "Appeal Notification Queue - Expedited Change" to “Appeals Notifications Queue” for member and provider appeal first level separately
		insert into #appeal_history_summary_filtered
		select Distinct @car_name car_name,  
		a.appeal_history_id,  
		a.auth_id,  
		a.appeal_type_id,  
		b.appeal_type_description,  
		c.appeal_contact_description,  
		dbo.initcap(e.lname + ', ' + e.fname) member_name,  
		dbo.initcap(g.lname + ', ' + g.fname) appeal_physician_name,   
		d.proc_desc,  
		a.appeal_received_date,  
		a.appeal_response_due_date,  
		dbo.initcap(f.lname + ', ' + f.fname) reviewing_physician_name,     
		a.expedited_appeal_flag,  
		h.appeal_status_description status_desc,  
		a.authorization_type_id,  
		d.tracking_number,  
		'' old_appeal_status_desc,  
		dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id) date_changed, 
		datediff(hour,dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id),getdate()) hours_in_queue,
		e.dob,    
		d.retro_flag,    
		d.cad_program_flag,  
		d.expedite_flag,  
		d.contact_type_id,  
		null line_of_business,     
		null fax_status_id,        
		null md_callout_status_id,  
		case when exists (select 1 from member_consenter (nolock) where member_id = e.member_id) then 1  
				else 0  
				end as has_member_representative,  
		dbo.uf_get_project(e.plan_id, 373) has_special_timeliness_requirement,  
		a.external_review_sent_date,  
		a.external_review_determination_date,  
		a.appeal_extended,  
		a.extend_notification_complete,
		a.expedited_review_requested  
		from appeal_history a (nolock)  
		inner join niacore..appeal_types b (nolock) on a.appeal_type_id = b.appeal_type_id  
		inner join niacore..appeal_contacts c (nolock) on a.appeal_contact_id = c.appeal_contact_id  
		inner join authorizations d (nolock) on a.auth_id = d.auth_id  
		inner join members e (nolock) on d.member_id = e.member_id  
		left outer join niacore..is_users f (nolock) on a.reviewing_physician_id = f.is_user_id   
		inner join physicians g (nolock) on d.phys_id = g.phys_id  
		inner join niacore..appeal_status_codes h (nolock) on a.appeal_status = h.appeal_status  
		inner join appeal_status_change i on a.appeal_history_id = i.appeal_history_id       
		inner join niacore..appeal_status_codes j (nolock) on i.old_appeal_status = j.appeal_status          
		inner join niacore..appeal_info k (nolock) on a.appeal_info_id = k.appeal_info_id   
		WHERE 
		(a.appeal_notification_complete is null or a.appeal_notification_complete = 0)   
		and a.expedited_appeal_flag = 1     
		and a.appeal_status in ('yt', 'yn')  
		--and e.plan_id in (select plan_id from niacore..plan_projects where  project_id = 576)  
		and dbo.uf_get_project(e.plan_id, 576) = 1  
		and a.appeal_type_id in ('p1','p2' )
   

		insert into #appeal_history_summary_filtered
		select Distinct @car_name car_name,  
		a.appeal_history_id,  
		a.auth_id,  
		a.appeal_type_id,  
		b.appeal_type_description,  
		c.appeal_contact_description,  
		dbo.initcap(e.lname + ', ' + e.fname) member_name,  
		dbo.initcap(g.lname + ', ' + g.fname) appeal_physician_name,   
		d.proc_desc,  
		a.appeal_received_date,  
		a.appeal_response_due_date,  
		dbo.initcap(f.lname + ', ' + f.fname) reviewing_physician_name,     
		a.expedited_appeal_flag,  
		h.appeal_status_description status_desc,  
		a.authorization_type_id,  
		d.tracking_number,  
		'' old_appeal_status_desc,  
		dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id) date_changed, 
		datediff(hour,dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id),getdate()) hours_in_queue,
		e.dob,    
		d.retro_flag,    
		d.cad_program_flag,  
		d.expedite_flag,  
		d.contact_type_id,  
		null line_of_business,     
		null fax_status_id,        
		null md_callout_status_id,  
		case when exists (select 1 from member_consenter (nolock) where member_id = e.member_id) then 1  
				else 0  
				end as has_member_representative,  
		dbo.uf_get_project(e.plan_id, 373) has_special_timeliness_requirement,  
		a.external_review_sent_date,  
		a.external_review_determination_date,  
		a.appeal_extended,  
		a.extend_notification_complete,
		a.expedited_review_requested  
		from appeal_history a (nolock)  
		inner join niacore..appeal_types b (nolock) on a.appeal_type_id = b.appeal_type_id  
		inner join niacore..appeal_contacts c (nolock) on a.appeal_contact_id = c.appeal_contact_id  
		inner join authorizations d (nolock) on a.auth_id = d.auth_id  
		inner join members e (nolock) on d.member_id = e.member_id  
		left outer join niacore..is_users f (nolock) on a.reviewing_physician_id = f.is_user_id   
		inner join physicians g (nolock) on d.phys_id = g.phys_id  
		inner join niacore..appeal_status_codes h (nolock) on a.appeal_status = h.appeal_status  
		inner join appeal_status_change i on a.appeal_history_id = i.appeal_history_id       
		inner join niacore..appeal_status_codes j (nolock) on i.old_appeal_status = j.appeal_status          
		inner join niacore..appeal_info k (nolock) on a.appeal_info_id = k.appeal_info_id   
		WHERE   
		(a.appeal_notification_complete is null or a.appeal_notification_complete = 0)   
		and a.expedited_appeal_flag = 1     
		and a.appeal_status in ('yt', 'yn')  
		and dbo.uf_get_project(e.plan_id, 662) = 1  
		and a.appeal_type_id in ('m1','m2')   
    
   
		----VJ changes TBLAZERS-195 to move standard retro appeals to “Appeals Notifications Queue”   
		-- and back to triage queue  
		--Thotan 09/21/2022 BSHBBI-3441 to move "Appeal Notification Queue - Standard Retro Change" to “Appeals Notifications Queue” for member and provider appeal first level separately 
			insert into #appeal_history_summary_filtered
		select Distinct @car_name car_name,  
		a.appeal_history_id,  
		a.auth_id,  
		a.appeal_type_id,  
		b.appeal_type_description,  
		c.appeal_contact_description,  
		dbo.initcap(e.lname + ', ' + e.fname) member_name,  
		dbo.initcap(g.lname + ', ' + g.fname) appeal_physician_name,   
		d.proc_desc,  
		a.appeal_received_date,  
		a.appeal_response_due_date,  
		dbo.initcap(f.lname + ', ' + f.fname) reviewing_physician_name,     
		a.expedited_appeal_flag,  
		h.appeal_status_description status_desc,  
		a.authorization_type_id,  
		d.tracking_number,  
		'' old_appeal_status_desc,  
		dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id) date_changed, 
		datediff(hour,dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id),getdate()) hours_in_queue,
		e.dob,    
		d.retro_flag,    
		d.cad_program_flag,  
		d.expedite_flag,  
		d.contact_type_id,  
		null line_of_business,     
		null fax_status_id,        
		null md_callout_status_id,  
		case when exists (select 1 from member_consenter (nolock) where member_id = e.member_id) then 1  
				else 0  
				end as has_member_representative,  
		dbo.uf_get_project(e.plan_id, 373) has_special_timeliness_requirement,  
		a.external_review_sent_date,  
		a.external_review_determination_date,  
		a.appeal_extended,  
		a.extend_notification_complete
		,a.expedited_review_requested  
		from appeal_history a (nolock)  
		inner join niacore..appeal_types b (nolock) on a.appeal_type_id = b.appeal_type_id  
		inner join niacore..appeal_contacts c (nolock) on a.appeal_contact_id = c.appeal_contact_id  
		inner join authorizations d (nolock) on a.auth_id = d.auth_id  
		inner join members e (nolock) on d.member_id = e.member_id  
		left outer join niacore..is_users f (nolock) on a.reviewing_physician_id = f.is_user_id   
		inner join physicians g (nolock) on d.phys_id = g.phys_id  
		inner join niacore..appeal_status_codes h (nolock) on a.appeal_status = h.appeal_status  
		inner join appeal_status_change i on a.appeal_history_id = i.appeal_history_id       
		inner join niacore..appeal_status_codes j (nolock) on i.old_appeal_status = j.appeal_status          
		inner join niacore..appeal_info k (nolock) on a.appeal_info_id = k.appeal_info_id   
		WHERE 
		(a.appeal_notification_complete is null or a.appeal_notification_complete = 0)   
		and a.expedited_appeal_flag = 0     
		and a.appeal_status in ('yt', 'yn', 'aq')  
		--and e.plan_id in (select plan_id from niacore..plan_projects where  project_id = 577)  
		and dbo.uf_get_project(e.plan_id, 577) = 1  
		and a.appeal_type_id in ('p1','p2')  
    
		insert into #appeal_history_summary_filtered
		select Distinct @car_name car_name,  
		a.appeal_history_id,  
		a.auth_id,  
		a.appeal_type_id,  
		b.appeal_type_description,  
		c.appeal_contact_description,  
		dbo.initcap(e.lname + ', ' + e.fname) member_name,  
		dbo.initcap(g.lname + ', ' + g.fname) appeal_physician_name,   
		d.proc_desc,  
		a.appeal_received_date,  
		a.appeal_response_due_date,  
		dbo.initcap(f.lname + ', ' + f.fname) reviewing_physician_name,     
		a.expedited_appeal_flag,  
		h.appeal_status_description status_desc,  
		a.authorization_type_id,  
		d.tracking_number,  
		'' old_appeal_status_desc,  
		dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id) date_changed, 
		datediff(hour,dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id),getdate()) hours_in_queue,
		e.dob,    
		d.retro_flag,    
		d.cad_program_flag,  
		d.expedite_flag,  
		d.contact_type_id,  
		null line_of_business,     
		null fax_status_id,        
		null md_callout_status_id,  
		case when exists (select 1 from member_consenter (nolock) where member_id = e.member_id) then 1  
				else 0  
				end as has_member_representative,  
		dbo.uf_get_project(e.plan_id, 373) has_special_timeliness_requirement,  
		a.external_review_sent_date,  
		a.external_review_determination_date,  
		a.appeal_extended,  
		a.extend_notification_complete,
		a.expedited_review_requested  
		from appeal_history a (nolock)  
		inner join niacore..appeal_types b (nolock) on a.appeal_type_id = b.appeal_type_id  
		inner join niacore..appeal_contacts c (nolock) on a.appeal_contact_id = c.appeal_contact_id  
		inner join authorizations d (nolock) on a.auth_id = d.auth_id  
		inner join members e (nolock) on d.member_id = e.member_id  
		left outer join niacore..is_users f (nolock) on a.reviewing_physician_id = f.is_user_id   
		inner join physicians g (nolock) on d.phys_id = g.phys_id  
		inner join niacore..appeal_status_codes h (nolock) on a.appeal_status = h.appeal_status  
		inner join appeal_status_change i on a.appeal_history_id = i.appeal_history_id       
		inner join niacore..appeal_status_codes j (nolock) on i.old_appeal_status = j.appeal_status          
		inner join niacore..appeal_info k (nolock) on a.appeal_info_id = k.appeal_info_id   
		WHERE  
		(a.appeal_notification_complete is null or a.appeal_notification_complete = 0)   
		and a.expedited_appeal_flag = 0     
		and a.appeal_status in ('yt', 'yn', 'aq')  
		and dbo.uf_get_project(e.plan_id, 663) = 1  
		and a.appeal_type_id in ('m1','m2') 
     
   
		--VJ changes TBLAZERS-195 to move Expedited Appeal Review not granted appeals to “Appeals Notifications Queue”   
		--thotan BSHBBI-3430 updating project_id 578 to move Expedited Appeal Review not granted appeals to “Appeals Notifications Queue” for Provider
		--Thotan 9/15/2022 Rolling back the changes for BSHBBI-3430 Expedited Appeal Notification for Expedited not granted 
		--Thotan 09/20/2022 BSHBBI-3430 Enabling the rolled back changes for Expedited Appeal Notification for Expedited not granted
		--Thotan BSHBBI-4174 To only route expedited appeals to Appeal Notification Queue when appeal is completed.
		insert into #appeal_history_summary_filtered
		select Distinct @car_name car_name,  
		a.appeal_history_id,  
		a.auth_id,  
		a.appeal_type_id,  
		b.appeal_type_description,  
		c.appeal_contact_description,  
		dbo.initcap(e.lname + ', ' + e.fname) member_name,  
		dbo.initcap(g.lname + ', ' + g.fname) appeal_physician_name,   
		d.proc_desc,  
		a.appeal_received_date,  
		a.appeal_response_due_date,  
		dbo.initcap(f.lname + ', ' + f.fname) reviewing_physician_name,     
		a.expedited_appeal_flag,  
		h.appeal_status_description status_desc,  
		a.authorization_type_id,  
		d.tracking_number,  
		'' old_appeal_status_desc,  
		dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id) date_changed, 
		datediff(hour,dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id),getdate()) hours_in_queue,
		e.dob,    
		d.retro_flag,    
		d.cad_program_flag,  
		d.expedite_flag,  
		d.contact_type_id,  
		null line_of_business,     
		null fax_status_id,        
		null md_callout_status_id,  
		case when exists (select 1 from member_consenter (nolock) where member_id = e.member_id) then 1  
				else 0  
				end as has_member_representative,  
		dbo.uf_get_project(e.plan_id, 373) has_special_timeliness_requirement,  
		a.external_review_sent_date,  
		a.external_review_determination_date,  
		a.appeal_extended,  
		a.extend_notification_complete,
		a.expedited_review_requested  
		from appeal_history a (nolock)  
		inner join niacore..appeal_types b (nolock) on a.appeal_type_id = b.appeal_type_id  
		inner join niacore..appeal_contacts c (nolock) on a.appeal_contact_id = c.appeal_contact_id  
		inner join authorizations d (nolock) on a.auth_id = d.auth_id  
		inner join members e (nolock) on d.member_id = e.member_id  
		left outer join niacore..is_users f (nolock) on a.reviewing_physician_id = f.is_user_id   
		inner join physicians g (nolock) on d.phys_id = g.phys_id  
		inner join niacore..appeal_status_codes h (nolock) on a.appeal_status = h.appeal_status  
		inner join appeal_status_change i on a.appeal_history_id = i.appeal_history_id       
		inner join niacore..appeal_status_codes j (nolock) on i.old_appeal_status = j.appeal_status          
		inner join niacore..appeal_info k (nolock) on a.appeal_info_id = k.appeal_info_id   
		WHERE  
		(a.appeal_notification_complete is null or a.appeal_notification_complete = 0)   
		and (a.expedited_appeal_flag = 0 or a.expedited_appeal_flag is null)
		and expedite_not_granted_flag = 1      
		and a.appeal_status in ('yt', 'yn','yp')  
		and (a.appeal_type_id in ('p1','p2','uk')) 
		--and e.plan_id in (select plan_id from niacore..plan_projects where  project_id = 578)  
		and dbo.uf_get_project(e.plan_id, 578) = 1  
    
        
		--TDM BSHBBI-1452: move Extended Appeal to “Appeals Notifications Queue”  
		insert into #appeal_history_summary_filtered
		select Distinct @car_name car_name,  
		a.appeal_history_id,  
		a.auth_id,  
		a.appeal_type_id,  
		b.appeal_type_description,  
		c.appeal_contact_description,  
		dbo.initcap(e.lname + ', ' + e.fname) member_name,  
		dbo.initcap(g.lname + ', ' + g.fname) appeal_physician_name,   
		d.proc_desc,  
		a.appeal_received_date,  
		a.appeal_response_due_date,  
		dbo.initcap(f.lname + ', ' + f.fname) reviewing_physician_name,     
		a.expedited_appeal_flag,  
		h.appeal_status_description status_desc,  
		a.authorization_type_id,  
		d.tracking_number,  
		'' old_appeal_status_desc,  
		dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id) date_changed, 
		datediff(hour,dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id),getdate()) hours_in_queue,
		e.dob,    
		d.retro_flag,    
		d.cad_program_flag,  
		d.expedite_flag,  
		d.contact_type_id,  
		null line_of_business,     
		null fax_status_id,        
		null md_callout_status_id,  
		case when exists (select 1 from member_consenter (nolock) where member_id = e.member_id) then 1  
				else 0  
				end as has_member_representative,  
		dbo.uf_get_project(e.plan_id, 373) has_special_timeliness_requirement,  
		a.external_review_sent_date,  
		a.external_review_determination_date,  
		a.appeal_extended,  
		a.extend_notification_complete,
		a.expedited_appeal_flag
		from appeal_history a (nolock)  
		inner join niacore..appeal_types b (nolock) on a.appeal_type_id = b.appeal_type_id  
		inner join niacore..appeal_contacts c (nolock) on a.appeal_contact_id = c.appeal_contact_id  
		inner join authorizations d (nolock) on a.auth_id = d.auth_id  
		inner join members e (nolock) on d.member_id = e.member_id  
		left outer join niacore..is_users f (nolock) on a.reviewing_physician_id = f.is_user_id   
		inner join physicians g (nolock) on d.phys_id = g.phys_id  
		inner join niacore..appeal_status_codes h (nolock) on a.appeal_status = h.appeal_status  
		inner join appeal_status_change i on a.appeal_history_id = i.appeal_history_id       
		inner join niacore..appeal_status_codes j (nolock) on i.old_appeal_status = j.appeal_status          
		inner join niacore..appeal_info k (nolock) on a.appeal_info_id = k.appeal_info_id   
		WHERE  
		(a.extend_notification_complete is null or a.extend_notification_complete = 0)   
		and a.appeal_extended = 1   
		and a.appeal_status in ('yt', 'yn','yp','pi','aq')  
		and a.appeal_type_id in ('m1','m2','p1','p2')  
		and dbo.uf_get_project(e.plan_id, 614) = 1  
    
		--Thotan 09/20/2022 BSHBBI-3430 Enabling the rolled back changes for Expedited Appeal Notification for Expedited not granted
		--Thotan 9/15/2022 Rolling back the changes for BSHBBI-3430 Expedited Appeal Notification for Expedited not granted 
		--thotan BSHBBI-3430 project_id 659 to move Expedited Appeal Review not granted appeals to “Appeals Notifications Queue” for Member 
		--Thotan BSHBBI-4174 To only route expedited appeals to Appeal Notification Queue when appeal is completed.
    
  
		insert into #appeal_history_summary_filtered
		select Distinct @car_name car_name,  
		a.appeal_history_id,  
		a.auth_id,  
		a.appeal_type_id,  
		b.appeal_type_description,  
		c.appeal_contact_description,  
		dbo.initcap(e.lname + ', ' + e.fname) member_name,  
		dbo.initcap(g.lname + ', ' + g.fname) appeal_physician_name,   
		d.proc_desc,  
		a.appeal_received_date,  
		a.appeal_response_due_date,  
		dbo.initcap(f.lname + ', ' + f.fname) reviewing_physician_name,     
		a.expedited_appeal_flag,  
		h.appeal_status_description status_desc,  
		a.authorization_type_id,  
		d.tracking_number,  
		'' old_appeal_status_desc,  
		dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id) date_changed, 
		datediff(hour,dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id),getdate()) hours_in_queue,
		e.dob,    
		d.retro_flag,    
		d.cad_program_flag,  
		d.expedite_flag,  
		d.contact_type_id,  
		null line_of_business,     
		null fax_status_id,        
		null md_callout_status_id,  
		case when exists (select 1 from member_consenter (nolock) where member_id = e.member_id) then 1  
				else 0  
				end as has_member_representative,  
		dbo.uf_get_project(e.plan_id, 373) has_special_timeliness_requirement,  
		a.external_review_sent_date,  
		a.external_review_determination_date,  
		a.appeal_extended,  
		a.extend_notification_complete,
		a.expedited_review_requested  
		from appeal_history a (nolock)  
		inner join niacore..appeal_types b (nolock) on a.appeal_type_id = b.appeal_type_id  
		inner join niacore..appeal_contacts c (nolock) on a.appeal_contact_id = c.appeal_contact_id  
		inner join authorizations d (nolock) on a.auth_id = d.auth_id  
		inner join members e (nolock) on d.member_id = e.member_id  
		left outer join niacore..is_users f (nolock) on a.reviewing_physician_id = f.is_user_id   
		inner join physicians g (nolock) on d.phys_id = g.phys_id  
		inner join niacore..appeal_status_codes h (nolock) on a.appeal_status = h.appeal_status  
		inner join appeal_status_change i on a.appeal_history_id = i.appeal_history_id       
		inner join niacore..appeal_status_codes j (nolock) on i.old_appeal_status = j.appeal_status          
		inner join niacore..appeal_info k (nolock) on a.appeal_info_id = k.appeal_info_id   
		WHERE   
		(a.appeal_notification_complete is null or a.appeal_notification_complete = 0) 
		and (a.expedited_appeal_flag = 0 or a.expedited_appeal_flag is null)
		and expedite_not_granted_flag = 1      
		and a.appeal_status in ('yt', 'yn','yp')  
		and (a.appeal_type_id in ('m1','m2','uk')) 
		and dbo.uf_get_project(e.plan_id, 659) = 1    
   
   
		--thotan BSHBBI-4357 to move "Appeal Notification Queue - Expedited Appeal Review not Granted Change for Member appeal and Member Contact Type" to “Appeals Notifications Queue” for member appeal first level.
		insert into #appeal_history_summary_filtered
		select Distinct @car_name car_name,  
		a.appeal_history_id,  
		a.auth_id,  
		a.appeal_type_id,  
		b.appeal_type_description,  
		c.appeal_contact_description,  
		dbo.initcap(e.lname + ', ' + e.fname) member_name,  
		dbo.initcap(g.lname + ', ' + g.fname) appeal_physician_name,   
		d.proc_desc,  
		a.appeal_received_date,  
		a.appeal_response_due_date,  
		dbo.initcap(f.lname + ', ' + f.fname) reviewing_physician_name,     
		a.expedited_appeal_flag,  
		h.appeal_status_description status_desc,  
		a.authorization_type_id,  
		d.tracking_number,  
		'' old_appeal_status_desc,  
		dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id) date_changed, 
		datediff(hour,dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id),getdate()) hours_in_queue,
		e.dob,    
		d.retro_flag,    
		d.cad_program_flag,  
		d.expedite_flag,  
		d.contact_type_id,  
		null line_of_business,     
		null fax_status_id,        
		null md_callout_status_id,  
		case when exists (select 1 from member_consenter (nolock) where member_id = e.member_id) then 1  
				else 0  
				end as has_member_representative,  
		dbo.uf_get_project(e.plan_id, 373) has_special_timeliness_requirement,  
		a.external_review_sent_date,  
		a.external_review_determination_date,  
		a.appeal_extended,  
		a.extend_notification_complete,
		a.expedited_review_requested  
		from appeal_history a (nolock)  
		inner join niacore..appeal_types b (nolock) on a.appeal_type_id = b.appeal_type_id  
		inner join niacore..appeal_contacts c (nolock) on a.appeal_contact_id = c.appeal_contact_id  
		inner join authorizations d (nolock) on a.auth_id = d.auth_id  
		inner join members e (nolock) on d.member_id = e.member_id  
		left outer join niacore..is_users f (nolock) on a.reviewing_physician_id = f.is_user_id   
		inner join physicians g (nolock) on d.phys_id = g.phys_id  
		inner join niacore..appeal_status_codes h (nolock) on a.appeal_status = h.appeal_status  
		inner join appeal_status_change i on a.appeal_history_id = i.appeal_history_id       
		inner join niacore..appeal_status_codes j (nolock) on i.old_appeal_status = j.appeal_status          
		inner join niacore..appeal_info k (nolock) on a.appeal_info_id = k.appeal_info_id   
		WHERE   
		(a.appeal_notification_complete is null or a.appeal_notification_complete = 0) 
		and (a.expedited_appeal_flag = 0 or a.expedited_appeal_flag is null)
		and expedite_not_granted_flag = 1      
		and a.appeal_status in ('yt', 'yn','yp')  
		and (a.appeal_type_id in ('m1','m2','uk')) --or (a.appeal_type_id = 'uk' and a.appeal_contact_id = 1))
		and a.appeal_contact_id = 1
		and dbo.uf_get_project(e.plan_id, 679) = 1

		order by  a.expedited_appeal_flag desc, a.appeal_response_due_date asc  

		-- NG 09/08/23 calling SP  usp_select_appeal_history_queues which inserts data for each auth belonging to various appeal queues
		select  *,
			RowNum = row_number() OVER ( order by auth_id )
		into #tmpAppeal
		from   #appeal_history_summary_filtered 

		select @cnt= 0
		select @cnt = count(*) from #tmpAppeal

		select @counter = 0

		while @counter <= @cnt
			begin
			select @appeal_auth_id = auth_id from #tmpAppeal
			where RowNum = @counter
			exec usp_select_appeal_history_queues @appeal_auth_id
			select @counter = @counter+1
		end

		IF OBJECT_ID('tempdb..#tmpAppeal') IS NOT NULL
		drop table #tmpAppeal
		--- End

		select distinct *,
			    '' as dt_of_last_doc_rcvd, 
				auth_queue = dbo.uf_select_auth_isfinal_queue(auth_id),
				auth_appeal_queues = dbo.udf_get_appeal_history_queues(appeal_history_id,@car_id)
		from #appeal_history_summary_filtered
		return

	end  -- data_filter_definition_id = 17 and appeal_type_id is null or ''
	else if @data_filter_definitions_id = 19
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
	else  
	begin  -- appeal type id is not null for data definition id 17
		 insert into #appeal_history_summary_filtered
		 select Distinct @car_name car_name,  
		   a.appeal_history_id,  
		   a.auth_id,  
		   a.appeal_type_id,  
		   b.appeal_type_description,  
		   c.appeal_contact_description,  
		   dbo.initcap(e.lname + ', ' + e.fname) member_name,  
		   dbo.initcap(g.lname + ', ' + g.fname) appeal_physician_name,   
		   d.proc_desc,  
		   a.appeal_received_date,  
		   a.appeal_response_due_date,  
		   dbo.initcap(f.lname + ', ' + f.fname) reviewing_physician_name,     
		   a.expedited_appeal_flag,  
		   h.appeal_status_description status_desc,  
		   a.authorization_type_id,  
		   d.tracking_number,  
		   '' old_appeal_status_desc,  
		   dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id) date_changed,  
		   datediff(hour,dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id),getdate()) hours_in_queue,
		   e.dob,   --4/04/2013 Fn  
		   d.retro_flag,  --4/04/2013 Fn  
		   d.cad_program_flag,  --4/04/2013 Fn  
		   d.expedite_flag,  --4/04/2013 Fn  
		   d.contact_type_id,  --4/04/2013 Fn  
		   null line_of_business,     --5/22/2013 Fn  
		   null fax_status_id,        --5/22/2013 Fn  
		   null md_callout_status_id,  --5/22/2013 Fn  
			case when exists (select 1 from member_consenter (nolock) where member_id = e.member_id) then 1  
				 else 0  
				 end as has_member_representative,  
			dbo.uf_get_project(e.plan_id, 373) has_special_timeliness_requirement, --03/27/18 UMT-82  
		   a.external_review_sent_date,  
		   a.external_review_determination_date,  
		   a.appeal_extended,  
		   a.extend_notification_complete,
		   a.expedited_review_requested  
		 from appeal_history a (nolock)  
		   inner join niacore..appeal_types b (nolock) on a.appeal_type_id = b.appeal_type_id  
		   inner join niacore..appeal_contacts c (nolock) on a.appeal_contact_id = c.appeal_contact_id  
		   inner join authorizations d (nolock) on a.auth_id = d.auth_id  
		   inner join members e (nolock) on d.member_id = e.member_id  
		   left outer join niacore..is_users f (nolock) on a.reviewing_physician_id = f.is_user_id   
		   inner join physicians g (nolock) on d.phys_id = g.phys_id  
		   inner join niacore..appeal_status_codes h (nolock) on a.appeal_status = h.appeal_status  
		   inner join appeal_status_change i on a.appeal_history_id = i.appeal_history_id     --Fnoll 7/05/2012   
		   inner join niacore..appeal_status_codes j (nolock) on i.old_appeal_status = j.appeal_status         --Fnoll 7/05/2012   
		   inner join niacore..appeal_info k (nolock) on a.appeal_info_id = k.appeal_info_id   -- Fnoll 07-29-2020 M360ENH-2577  
		 where a.appeal_status in (select a.assignment  
			   from niacore..data_filter_assignments a (nolock) ,  
				 niacore..data_filter_definitions b (nolock) ,  
				 niacore..data_filter_clients c (nolock)   
			   where a.data_filter_definitions_id = b.data_filter_definitions_id  
			   and b.data_filter_definitions_id = @data_filter_definitions_id  
			   and b.data_filter_definitions_id = c.data_filter_definition_id   
			   and c.car_id = @car_id)  
		   and a.appeal_type_id = @appeal_type_id  
			 --VJ changes BSHBBI-804 to move appeals to “Appeals Notifications Queue”   
							and i.isfinal = 1                   
							 and    not exists (select 'true' from niacore..vw_auth_type_terminated_plans   
																			 where plan_id = e.plan_id  
																			 and authorization_type_id = a.authorization_type_id)  
							 and (a.appeal_notification_complete is null or a.appeal_notification_complete = 0)   
							 and (a.expedited_appeal_flag = 1 or k.appeal_notification_queue_all = 1)  
							 and a.created_date >= @queue_begin_date  
							 and a.appeal_status in ('ax','ay','az')    --2021/07/15  
							 --or  
							 --and  
							 --VJ changes TBLAZERS-195 to move standard retro appeals to “Appeals Notifications Queue”   
							 -- and back to completed queue  
							 --(  
							 --(a.appeal_notification_complete is null or a.appeal_notification_complete = 0)   
							 --and a.expedited_appeal_flag = 0   
							 --and  k.appeal_notification_queue_all = 1                      
							 --and a.appeal_status in ('ax','ay','az')  
							 --)      
     
		   --VJ changes TBLAZERS-195 to move expedited appeals to “Appeals Notifications Queue”  
		   --Thotan 09/21/2022 BSHBBI-3440 to move "Appeal Notification Queue - Expedited Change" to “Appeals Notifications Queue” for member and provider appeal first level separately 
			insert into #appeal_history_summary_filtered
		 select Distinct @car_name car_name,  
		   a.appeal_history_id,  
		   a.auth_id,  
		   a.appeal_type_id,  
		   b.appeal_type_description,  
		   c.appeal_contact_description,  
		   dbo.initcap(e.lname + ', ' + e.fname) member_name,  
		   dbo.initcap(g.lname + ', ' + g.fname) appeal_physician_name,   
		   d.proc_desc,  
		   a.appeal_received_date,  
		   a.appeal_response_due_date,  
		   dbo.initcap(f.lname + ', ' + f.fname) reviewing_physician_name,     
		   a.expedited_appeal_flag,  
		   h.appeal_status_description status_desc,  
		   a.authorization_type_id,  
		   d.tracking_number,  
		   '' old_appeal_status_desc,  
		   dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id) date_changed, 
		   datediff(hour,dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id),getdate()) hours_in_queue,
		   e.dob,    
		   d.retro_flag,    
		   d.cad_program_flag,  
		   d.expedite_flag,  
		   d.contact_type_id,  
		   null line_of_business,     
		   null fax_status_id,        
		   null md_callout_status_id,  
			case when exists (select 1 from member_consenter (nolock) where member_id = e.member_id) then 1  
				 else 0  
				 end as has_member_representative,  
			dbo.uf_get_project(e.plan_id, 373) has_special_timeliness_requirement,  
		   a.external_review_sent_date,  
		   a.external_review_determination_date,  
		   a.appeal_extended,  
		   a.extend_notification_complete,
		   a.expedited_review_requested  
		 from appeal_history a (nolock)  
		   inner join niacore..appeal_types b (nolock) on a.appeal_type_id = b.appeal_type_id  
		   inner join niacore..appeal_contacts c (nolock) on a.appeal_contact_id = c.appeal_contact_id  
		   inner join authorizations d (nolock) on a.auth_id = d.auth_id  
		   inner join members e (nolock) on d.member_id = e.member_id  
		   left outer join niacore..is_users f (nolock) on a.reviewing_physician_id = f.is_user_id   
		   inner join physicians g (nolock) on d.phys_id = g.phys_id  
		   inner join niacore..appeal_status_codes h (nolock) on a.appeal_status = h.appeal_status  
		   inner join appeal_status_change i on a.appeal_history_id = i.appeal_history_id       
		   inner join niacore..appeal_status_codes j (nolock) on i.old_appeal_status = j.appeal_status          
		   inner join niacore..appeal_info k (nolock) on a.appeal_info_id = k.appeal_info_id   
		   WHERE 
		   (a.appeal_notification_complete is null or a.appeal_notification_complete = 0)   
		   and a.expedited_appeal_flag = 1     
		   and a.appeal_status in ('yt', 'yn')  
		   --and e.plan_id in (select plan_id from niacore..plan_projects where  project_id = 576)  
		   and dbo.uf_get_project(e.plan_id, 576) = 1  
		   and a.appeal_type_id in ('p1','p2' )  
		   and a.appeal_type_id = @appeal_type_id 
     
			insert into #appeal_history_summary_filtered
		 select Distinct @car_name car_name,  
		   a.appeal_history_id,  
		   a.auth_id,  
		   a.appeal_type_id,  
		   b.appeal_type_description,  
		   c.appeal_contact_description,  
		   dbo.initcap(e.lname + ', ' + e.fname) member_name,  
		   dbo.initcap(g.lname + ', ' + g.fname) appeal_physician_name,   
		   d.proc_desc,  
		   a.appeal_received_date,  
		   a.appeal_response_due_date,  
		   dbo.initcap(f.lname + ', ' + f.fname) reviewing_physician_name,     
		   a.expedited_appeal_flag,  
		   h.appeal_status_description status_desc,  
		   a.authorization_type_id,  
		   d.tracking_number,  
		   '' old_appeal_status_desc,  
		   dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id) date_changed, 
		   datediff(hour,dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id),getdate()) hours_in_queue,
		   e.dob,    
		   d.retro_flag,    
		   d.cad_program_flag,  
		   d.expedite_flag,  
		   d.contact_type_id,  
		   null line_of_business,     
		   null fax_status_id,        
		   null md_callout_status_id,  
			case when exists (select 1 from member_consenter (nolock) where member_id = e.member_id) then 1  
				 else 0  
				 end as has_member_representative,  
			dbo.uf_get_project(e.plan_id, 373) has_special_timeliness_requirement,  
		   a.external_review_sent_date,  
		   a.external_review_determination_date,  
		   a.appeal_extended,  
		   a.extend_notification_complete,
		   a.expedited_review_requested  
		 from appeal_history a (nolock)  
		   inner join niacore..appeal_types b (nolock) on a.appeal_type_id = b.appeal_type_id  
		   inner join niacore..appeal_contacts c (nolock) on a.appeal_contact_id = c.appeal_contact_id  
		   inner join authorizations d (nolock) on a.auth_id = d.auth_id  
		   inner join members e (nolock) on d.member_id = e.member_id  
		   left outer join niacore..is_users f (nolock) on a.reviewing_physician_id = f.is_user_id   
		   inner join physicians g (nolock) on d.phys_id = g.phys_id  
		   inner join niacore..appeal_status_codes h (nolock) on a.appeal_status = h.appeal_status  
		   inner join appeal_status_change i on a.appeal_history_id = i.appeal_history_id       
		   inner join niacore..appeal_status_codes j (nolock) on i.old_appeal_status = j.appeal_status          
		   inner join niacore..appeal_info k (nolock) on a.appeal_info_id = k.appeal_info_id   
		   WHERE  
		   (a.appeal_notification_complete is null or a.appeal_notification_complete = 0)   
		   and a.expedited_appeal_flag = 1     
		   and a.appeal_status in ('yt', 'yn')    
		   and dbo.uf_get_project(e.plan_id, 662) = 1  
		   and a.appeal_type_id in ('m1','m2')
		   and a.appeal_type_id = @appeal_type_id 
    
   
		   ----VJ changes TBLAZERS-195 to move standard retro appeals to “Appeals Notifications Queue”   
		   -- and back to triage queue  
		   --Thotan 09/21/2022 BSHBBI-3441 to move "Appeal Notification Queue - Standard Retro Change" to “Appeals Notifications Queue” for member and provider appeal first level separately 
			insert into #appeal_history_summary_filtered
		 select Distinct @car_name car_name,  
		   a.appeal_history_id,  
		   a.auth_id,  
		   a.appeal_type_id,  
		   b.appeal_type_description,  
		   c.appeal_contact_description,  
		   dbo.initcap(e.lname + ', ' + e.fname) member_name,  
		   dbo.initcap(g.lname + ', ' + g.fname) appeal_physician_name,   
		   d.proc_desc,  
		   a.appeal_received_date,  
		   a.appeal_response_due_date,  
		   dbo.initcap(f.lname + ', ' + f.fname) reviewing_physician_name,     
		   a.expedited_appeal_flag,  
		   h.appeal_status_description status_desc,  
		   a.authorization_type_id,  
		   d.tracking_number,  
		   '' old_appeal_status_desc,  
		   dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id) date_changed, 
		   datediff(hour,dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id),getdate()) hours_in_queue,
		   e.dob,    
		   d.retro_flag,    
		   d.cad_program_flag,  
		   d.expedite_flag,  
		   d.contact_type_id,  
		   null line_of_business,     
		   null fax_status_id,        
		   null md_callout_status_id,  
			case when exists (select 1 from member_consenter (nolock) where member_id = e.member_id) then 1  
				 else 0  
				 end as has_member_representative,  
			dbo.uf_get_project(e.plan_id, 373) has_special_timeliness_requirement,  
		   a.external_review_sent_date,  
		   a.external_review_determination_date,  
		   a.appeal_extended,  
		   a.extend_notification_complete,
		   a.expedited_review_requested  
		 from appeal_history a (nolock)  
		   inner join niacore..appeal_types b (nolock) on a.appeal_type_id = b.appeal_type_id  
		   inner join niacore..appeal_contacts c (nolock) on a.appeal_contact_id = c.appeal_contact_id  
		   inner join authorizations d (nolock) on a.auth_id = d.auth_id  
		   inner join members e (nolock) on d.member_id = e.member_id  
		   left outer join niacore..is_users f (nolock) on a.reviewing_physician_id = f.is_user_id   
		   inner join physicians g (nolock) on d.phys_id = g.phys_id  
		   inner join niacore..appeal_status_codes h (nolock) on a.appeal_status = h.appeal_status  
		   inner join appeal_status_change i on a.appeal_history_id = i.appeal_history_id       
		   inner join niacore..appeal_status_codes j (nolock) on i.old_appeal_status = j.appeal_status          
		   inner join niacore..appeal_info k (nolock) on a.appeal_info_id = k.appeal_info_id   
		   WHERE  
		   (a.appeal_notification_complete is null or a.appeal_notification_complete = 0)   
		   and a.expedited_appeal_flag = 0     
		   and a.appeal_status in ('yt', 'yn', 'aq')  
		   --and e.plan_id in (select plan_id from niacore..plan_projects where  project_id = 577)  
		   and dbo.uf_get_project(e.plan_id, 577) = 1  
		   and a.appeal_type_id in ('p1','p2') 
		   and a.appeal_type_id = @appeal_type_id 
     
			insert into #appeal_history_summary_filtered
		 select Distinct @car_name car_name,  
		   a.appeal_history_id,  
		   a.auth_id,  
		   a.appeal_type_id,  
		   b.appeal_type_description,  
		   c.appeal_contact_description,  
		   dbo.initcap(e.lname + ', ' + e.fname) member_name,  
		   dbo.initcap(g.lname + ', ' + g.fname) appeal_physician_name,   
		   d.proc_desc,  
		   a.appeal_received_date,  
		   a.appeal_response_due_date,  
		   dbo.initcap(f.lname + ', ' + f.fname) reviewing_physician_name,     
		   a.expedited_appeal_flag,  
		   h.appeal_status_description status_desc,  
		   a.authorization_type_id,  
		   d.tracking_number,  
		   '' old_appeal_status_desc,  
		   dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id) date_changed, 
		   datediff(hour,dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id),getdate()) hours_in_queue,
		   e.dob,    
		   d.retro_flag,    
		   d.cad_program_flag,  
		   d.expedite_flag,  
		   d.contact_type_id,  
		   null line_of_business,     
		   null fax_status_id,        
		   null md_callout_status_id,  
			case when exists (select 1 from member_consenter (nolock) where member_id = e.member_id) then 1  
				 else 0  
				 end as has_member_representative,  
			dbo.uf_get_project(e.plan_id, 373) has_special_timeliness_requirement,  
		   a.external_review_sent_date,  
		   a.external_review_determination_date,  
		   a.appeal_extended,  
		   a.extend_notification_complete,
		   a.expedited_review_requested  
		 from appeal_history a (nolock)  
		   inner join niacore..appeal_types b (nolock) on a.appeal_type_id = b.appeal_type_id  
		   inner join niacore..appeal_contacts c (nolock) on a.appeal_contact_id = c.appeal_contact_id  
		   inner join authorizations d (nolock) on a.auth_id = d.auth_id  
		   inner join members e (nolock) on d.member_id = e.member_id  
		   left outer join niacore..is_users f (nolock) on a.reviewing_physician_id = f.is_user_id   
		   inner join physicians g (nolock) on d.phys_id = g.phys_id  
		   inner join niacore..appeal_status_codes h (nolock) on a.appeal_status = h.appeal_status  
		   inner join appeal_status_change i on a.appeal_history_id = i.appeal_history_id       
		   inner join niacore..appeal_status_codes j (nolock) on i.old_appeal_status = j.appeal_status          
		   inner join niacore..appeal_info k (nolock) on a.appeal_info_id = k.appeal_info_id   
		   WHERE  
		   (a.appeal_notification_complete is null or a.appeal_notification_complete = 0)   
		   and a.expedited_appeal_flag = 0     
		   and a.appeal_status in ('yt', 'yn', 'aq')    
		   and dbo.uf_get_project(e.plan_id, 663) = 1  
		   and a.appeal_type_id in ('m1','m2')
		   and a.appeal_type_id = @appeal_type_id 
    
		   --VJ changes TBLAZERS-195 to move Expedited Appeal Review not granted appeals to “Appeals Notifications Queue”  
		   --thotan BSHBBI-3430 updating project_id 578 to move Expedited Appeal Review not granted appeals to “Appeals Notifications Queue” for Provider
		   --Thotan 9/15/2022 Rolling back the changes for BSHBBI-3430 Expedited Appeal Notification for Expedited not granted 
		   --Thotan BSHBBI-4174 To only route expedited appeals to Appeal Notification Queue when appeal is completed.
			insert into #appeal_history_summary_filtered
		 select Distinct @car_name car_name,  
		   a.appeal_history_id,  
		   a.auth_id,  
		   a.appeal_type_id,  
		   b.appeal_type_description,  
		   c.appeal_contact_description,  
		   dbo.initcap(e.lname + ', ' + e.fname) member_name,  
		   dbo.initcap(g.lname + ', ' + g.fname) appeal_physician_name,   
		   d.proc_desc,  
		   a.appeal_received_date,  
		   a.appeal_response_due_date,  
		   dbo.initcap(f.lname + ', ' + f.fname) reviewing_physician_name,     
		   a.expedited_appeal_flag,  
		   h.appeal_status_description status_desc,  
		   a.authorization_type_id,  
		   d.tracking_number,  
		   '' old_appeal_status_desc,  
		   dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id) date_changed, 
		   datediff(hour,dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id),getdate()) hours_in_queue,
		   e.dob,    
		   d.retro_flag,    
		   d.cad_program_flag,  
		   d.expedite_flag,  
		   d.contact_type_id,  
		   null line_of_business,     
		   null fax_status_id,        
		   null md_callout_status_id,  
			case when exists (select 1 from member_consenter (nolock) where member_id = e.member_id) then 1  
				 else 0  
				 end as has_member_representative,  
			dbo.uf_get_project(e.plan_id, 373) has_special_timeliness_requirement,  
		   a.external_review_sent_date,  
		   a.external_review_determination_date,  
		   a.appeal_extended,  
		   a.extend_notification_complete,
		   a.expedited_review_requested  
		 from appeal_history a (nolock)  
		   inner join niacore..appeal_types b (nolock) on a.appeal_type_id = b.appeal_type_id  
		   inner join niacore..appeal_contacts c (nolock) on a.appeal_contact_id = c.appeal_contact_id  
		   inner join authorizations d (nolock) on a.auth_id = d.auth_id  
		   inner join members e (nolock) on d.member_id = e.member_id  
		   left outer join niacore..is_users f (nolock) on a.reviewing_physician_id = f.is_user_id   
		   inner join physicians g (nolock) on d.phys_id = g.phys_id  
		   inner join niacore..appeal_status_codes h (nolock) on a.appeal_status = h.appeal_status  
		   inner join appeal_status_change i on a.appeal_history_id = i.appeal_history_id       
		   inner join niacore..appeal_status_codes j (nolock) on i.old_appeal_status = j.appeal_status          
		   inner join niacore..appeal_info k (nolock) on a.appeal_info_id = k.appeal_info_id   
		   WHERE  
		   (a.appeal_notification_complete is null or a.appeal_notification_complete = 0)  
		   and (a.expedited_appeal_flag = 0 or a.expedited_appeal_flag is null)
		   and expedite_not_granted_flag = 1      
		   and a.appeal_status in ('yt', 'yn','yp')  
		   and (a.appeal_type_id in ('p1','p2','uk')) 
		   and a.appeal_type_id = @appeal_type_id 
		   --and e.plan_id in (select plan_id from niacore..plan_projects where  project_id = 578)  
		   and dbo.uf_get_project(e.plan_id, 578) = 1  
    
     
		   --TDM BSHBBI-1452: move Extended Appeal to “Appeals Notifications Queue”  
			insert into #appeal_history_summary_filtered
		 select Distinct @car_name car_name,  
		   a.appeal_history_id,  
		   a.auth_id,  
		   a.appeal_type_id,  
		   b.appeal_type_description,  
		   c.appeal_contact_description,  
		   dbo.initcap(e.lname + ', ' + e.fname) member_name,  
		   dbo.initcap(g.lname + ', ' + g.fname) appeal_physician_name,   
		   d.proc_desc,  
		   a.appeal_received_date,  
		   a.appeal_response_due_date,  
		   dbo.initcap(f.lname + ', ' + f.fname) reviewing_physician_name,     
		   a.expedited_appeal_flag,  
		   h.appeal_status_description status_desc,  
		   a.authorization_type_id,  
		   d.tracking_number,  
		   '' old_appeal_status_desc,  
		   dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id) date_changed, 
		   datediff(hour,dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id),getdate()) hours_in_queue,
		   e.dob,    
		   d.retro_flag,    
		   d.cad_program_flag,  
		   d.expedite_flag,  
		   d.contact_type_id,  
		   null line_of_business,     
		   null fax_status_id,        
		   null md_callout_status_id,  
			case when exists (select 1 from member_consenter (nolock) where member_id = e.member_id) then 1  
				 else 0  
				 end as has_member_representative,  
			dbo.uf_get_project(e.plan_id, 373) has_special_timeliness_requirement,  
		   a.external_review_sent_date,  
		   a.external_review_determination_date,  
		   a.appeal_extended,  
		   a.extend_notification_complete ,
		   a.expedited_appeal_flag
		 from appeal_history a (nolock)  
		   inner join niacore..appeal_types b (nolock) on a.appeal_type_id = b.appeal_type_id  
		   inner join niacore..appeal_contacts c (nolock) on a.appeal_contact_id = c.appeal_contact_id  
		   inner join authorizations d (nolock) on a.auth_id = d.auth_id  
		   inner join members e (nolock) on d.member_id = e.member_id  
		   left outer join niacore..is_users f (nolock) on a.reviewing_physician_id = f.is_user_id   
		   inner join physicians g (nolock) on d.phys_id = g.phys_id  
		   inner join niacore..appeal_status_codes h (nolock) on a.appeal_status = h.appeal_status  
		   inner join appeal_status_change i on a.appeal_history_id = i.appeal_history_id       
		   inner join niacore..appeal_status_codes j (nolock) on i.old_appeal_status = j.appeal_status          
		   inner join niacore..appeal_info k (nolock) on a.appeal_info_id = k.appeal_info_id   
		   WHERE 
		   (a.extend_notification_complete is null or a.extend_notification_complete = 0)   
		   and a.appeal_extended = 1   
		   and a.appeal_status in ('yt', 'yn','yp','pi','aq')  
		   and a.appeal_type_id in ('m1','m2','p1','p2')  
		   and a.appeal_type_id = @appeal_type_id  
		   and dbo.uf_get_project(e.plan_id, 614) = 1  
    
   
		   --Thotan 9/15/2022 Rolling back the changes for BSHBBI-3430 Expedited Appeal Notification for Expedited not granted   
		   --thotan BSHBBI-3430 project_id 659 to move Expedited Appeal Review not granted appeals to “Appeals Notifications Queue” for Member 
		   --Thotan BSHBBI-4174 To only route expedited appeals to Appeal Notification Queue when appeal is completed.
			 insert into #appeal_history_summary_filtered
		 select Distinct @car_name car_name,  
		   a.appeal_history_id,  
		   a.auth_id,  
		   a.appeal_type_id,  
		   b.appeal_type_description,  
		   c.appeal_contact_description,  
		   dbo.initcap(e.lname + ', ' + e.fname) member_name,  
		   dbo.initcap(g.lname + ', ' + g.fname) appeal_physician_name,   
		   d.proc_desc,  
		   a.appeal_received_date,  
		   a.appeal_response_due_date,  
		   dbo.initcap(f.lname + ', ' + f.fname) reviewing_physician_name,     
		   a.expedited_appeal_flag,  
		   h.appeal_status_description status_desc,  
		   a.authorization_type_id,  
		   d.tracking_number,  
		   '' old_appeal_status_desc,  
		   dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id) date_changed, 
		   datediff(hour,dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id),getdate()) hours_in_queue,
		   e.dob,    
		   d.retro_flag,    
		   d.cad_program_flag,  
		   d.expedite_flag,  
		   d.contact_type_id,  
		   null line_of_business,     
		   null fax_status_id,        
		   null md_callout_status_id,  
			case when exists (select 1 from member_consenter (nolock) where member_id = e.member_id) then 1  
				 else 0  
				 end as has_member_representative,  
			dbo.uf_get_project(e.plan_id, 373) has_special_timeliness_requirement,  
		   a.external_review_sent_date,  
		   a.external_review_determination_date,  
		   a.appeal_extended,  
		   a.extend_notification_complete,
		   a.expedited_review_requested  
		 from appeal_history a (nolock)  
		   inner join niacore..appeal_types b (nolock) on a.appeal_type_id = b.appeal_type_id  
		   inner join niacore..appeal_contacts c (nolock) on a.appeal_contact_id = c.appeal_contact_id  
		   inner join authorizations d (nolock) on a.auth_id = d.auth_id  
		   inner join members e (nolock) on d.member_id = e.member_id  
		   left outer join niacore..is_users f (nolock) on a.reviewing_physician_id = f.is_user_id   
		   inner join physicians g (nolock) on d.phys_id = g.phys_id  
		   inner join niacore..appeal_status_codes h (nolock) on a.appeal_status = h.appeal_status  
		   inner join appeal_status_change i on a.appeal_history_id = i.appeal_history_id       
		   inner join niacore..appeal_status_codes j (nolock) on i.old_appeal_status = j.appeal_status          
		   inner join niacore..appeal_info k (nolock) on a.appeal_info_id = k.appeal_info_id   
		   WHERE 
		   (a.appeal_notification_complete is null or a.appeal_notification_complete = 0)   
		   and (a.expedited_appeal_flag = 0 or a.expedited_appeal_flag is null)
		   and expedite_not_granted_flag = 1      
		   and a.appeal_status in ('yt', 'yn','yp')  
		   and (a.appeal_type_id in ('m1','m2', 'uk')) 
		   and a.appeal_type_id = @appeal_type_id 
		   and dbo.uf_get_project(e.plan_id, 659) = 1  


		   --thotan BSHBBI-4357 to move "Appeal Notification Queue - Expedited Appeal Review not Granted Change for Member appeal and Member Contact Type" to “Appeals Notifications Queue” for member appeal first level.
		   insert into #appeal_history_summary_filtered
		   select Distinct @car_name car_name,  
		   a.appeal_history_id,  
		   a.auth_id,  
		   a.appeal_type_id,  
		   b.appeal_type_description,  
		   c.appeal_contact_description,  
		   dbo.initcap(e.lname + ', ' + e.fname) member_name,  
		   dbo.initcap(g.lname + ', ' + g.fname) appeal_physician_name,   
		   d.proc_desc,  
		   a.appeal_received_date,  
		   a.appeal_response_due_date,  
		   dbo.initcap(f.lname + ', ' + f.fname) reviewing_physician_name,     
		   a.expedited_appeal_flag,  
		   h.appeal_status_description status_desc,  
		   a.authorization_type_id,  
		   d.tracking_number,  
		   '' old_appeal_status_desc,  
		   dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id) date_changed, 
		   datediff(hour,dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id),getdate()) hours_in_queue,
		   e.dob,    
		   d.retro_flag,    
		   d.cad_program_flag,  
		   d.expedite_flag,  
		   d.contact_type_id,  
		   null line_of_business,     
		   null fax_status_id,        
		   null md_callout_status_id,  
			case when exists (select 1 from member_consenter (nolock) where member_id = e.member_id) then 1  
				 else 0  
				 end as has_member_representative,  
			dbo.uf_get_project(e.plan_id, 373) has_special_timeliness_requirement,  
		   a.external_review_sent_date,  
		   a.external_review_determination_date,  
		   a.appeal_extended,  
		   a.extend_notification_complete,
		   a.expedited_review_requested  
		 from appeal_history a (nolock)  
		   inner join niacore..appeal_types b (nolock) on a.appeal_type_id = b.appeal_type_id  
		   inner join niacore..appeal_contacts c (nolock) on a.appeal_contact_id = c.appeal_contact_id  
		   inner join authorizations d (nolock) on a.auth_id = d.auth_id  
		   inner join members e (nolock) on d.member_id = e.member_id  
		   left outer join niacore..is_users f (nolock) on a.reviewing_physician_id = f.is_user_id   
		   inner join physicians g (nolock) on d.phys_id = g.phys_id  
		   inner join niacore..appeal_status_codes h (nolock) on a.appeal_status = h.appeal_status  
		   inner join appeal_status_change i on a.appeal_history_id = i.appeal_history_id       
		   inner join niacore..appeal_status_codes j (nolock) on i.old_appeal_status = j.appeal_status          
		   inner join niacore..appeal_info k (nolock) on a.appeal_info_id = k.appeal_info_id   
		   WHERE   
		   (a.appeal_notification_complete is null or a.appeal_notification_complete = 0) 
		   and (a.expedited_appeal_flag = 0 or a.expedited_appeal_flag is null)
		   and expedite_not_granted_flag = 1      
		   and a.appeal_status in ('yt', 'yn','yp')  
		   and (a.appeal_type_id in ('m1','m2','uk')) --or (a.appeal_type_id = 'uk' and a.appeal_contact_id = 1))
		   and a.appeal_contact_id = 1
		   and dbo.uf_get_project(e.plan_id, 679) = 1
		 order by  a.expedited_appeal_flag desc, a.appeal_response_due_date asc  

		 -- NG 09/08/23 calling SP  usp_select_appeal_history_queues which inserts data for each auth belonging to various appeal queues
		select  *,
			RowNum = row_number() OVER ( order by auth_id )
		into #tmpAppeal1
		from   #appeal_history_summary_filtered 

		select @cnt= 0
		select @cnt = count(*) from #tmpAppeal1

		select @counter = 0

		while @counter <= @cnt
			begin
			select @appeal_auth_id = auth_id from #tmpAppeal1
			where RowNum = @counter
			exec usp_select_appeal_history_queues @appeal_auth_id
			select @counter = @counter+1
		end

		IF OBJECT_ID('tempdb..#tmpAppeal1') IS NOT NULL
		drop table #tmpAppeal1


		--- End


		 select Distinct *,
		 ''as dt_of_last_doc_rcvd,
		  auth_queue = dbo.uf_select_auth_isfinal_queue(auth_id),
		  auth_appeal_queues = dbo.udf_get_appeal_history_queues(appeal_history_id,@car_id)from #appeal_history_summary_filtered
		 return
	 end  -- Data filter definition = 17 and appeal_type id is not null
end  -- Data filter definition = 17 ends here


-- Fnoll 11/14/2011 Exceptions coding for Language Review  
if @data_filter_definitions_id = 10  
 begin  
 set @data_filter_definitions_id = 4     --set the data filter to completed auths  
  if (@appeal_type_id  = '' or @appeal_type_id is null)   
  --We are selecting all the appeals status codes valid for that data filter.  
  insert into #appeal_history_summary_filtered
   select @car_name car_name,  
     a.appeal_history_id,  
     a.auth_id,  
     a.appeal_type_id,  
     b.appeal_type_description,  
     c.appeal_contact_description,  
     dbo.initcap(e.lname + ', ' + e.fname) member_name,  
     dbo.initcap(g.lname + ', ' + g.fname) appeal_physician_name,   
     d.proc_desc,  
     a.appeal_received_date,  
     a.appeal_response_due_date,  
     dbo.initcap(f.lname + ', ' + f.fname) reviewing_physician_name,     
     a.expedited_appeal_flag,  
     h.appeal_status_description status_desc,  
     a.authorization_type_id,  
     d.tracking_number,  
     j.appeal_status_description old_appeal_status_desc,  
     dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id) date_changed,  
	 datediff(hour,dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id),getdate()) hours_in_queue,
     e.dob,   --4/04/2013 Fn  
     d.retro_flag,  --4/04/2013 Fn  
     d.cad_program_flag,  --4/04/2013 Fn  
     d.expedite_flag,  --4/04/2013 Fn  
        d.contact_type_id,  --4/04/2013 Fn  
        null line_of_business,     --5/22/2013 Fn  
        null fax_status_id,        --5/22/2013 Fn  
        null md_callout_status_id,  --5/22/2013 Fn  
--     k.line_of_business,  --4/04/2013 Fn  
--     l.fax_status_id,  --4/04/2013 Fn  
--     l.md_callout_status_id --4/04/2013 Fn  
--VJ changes for tkt#199715   
     case when exists (select 1 from member_consenter (nolock) where member_id = e.member_id) then 1  
          else 0  
          end as has_member_representative,  
     dbo.uf_get_project(e.plan_id, 373) has_special_timeliness_requirement, --03/27/18 UMT-82  
     a.external_review_sent_date,  
     a.external_review_determination_date,  
     a.appeal_extended,  
     a.extend_notification_complete,
	 a.expedited_review_requested  
   from appeal_history a (nolock)  
     inner join niacore..appeal_types b (nolock) on a.appeal_type_id = b.appeal_type_id  
     inner join niacore..appeal_contacts c (nolock) on a.appeal_contact_id = c.appeal_contact_id  
     inner join authorizations d (nolock) on a.auth_id = d.auth_id  
     inner join members e (nolock) on d.member_id = e.member_id  
     left outer join niacore..is_users f (nolock) on a.reviewing_physician_id = f.is_user_id   
     inner join physicians g (nolock) on d.phys_id = g.phys_id  
     inner join niacore..appeal_status_codes h (nolock) on a.appeal_status = h.appeal_status  
     inner join appeal_status_change i (nolock) on a.appeal_history_id = i.appeal_history_id             --Fnoll 7/05/2012  
     inner join niacore..appeal_status_codes j (nolock) on i.old_appeal_status = j.appeal_status         --Fnoll 7/05/2012  
--     inner join niacore..appeal_info k (nolock) on a.appeal_info_id = k.appeal_info_id                   --Fnoll 4/04/2013   
--     left outer join auth_queue_values l (nolock) on d.auth_id = l.auth_id                               --Fnoll 4/04/2013   
   where a.appeal_status in (select a.assignment  
      from niacore..data_filter_assignments a (nolock),  
        niacore..data_filter_definitions b (nolock),  
        niacore..data_filter_clients c (nolock)  
      where a.data_filter_definitions_id = b.data_filter_definitions_id  
      and b.data_filter_definitions_id = @data_filter_definitions_id   
      and b.data_filter_definitions_id = c.data_filter_definition_id   
      and c.car_id = @car_id)  
--       and a.authorization_type_id = @authorization_type_id  
       and upper(a.language_review) = 'R'  
       and i.isfinal = 1                          
        -- NG 08/29/17 hide auths for the carriers terminated for specific auth type id.  
     and  not exists (select 'true' from niacore..vw_auth_type_terminated_plans   
            where plan_id = e.plan_id  
            and authorization_type_id = a.authorization_type_id)  
   order by a.expedited_appeal_flag desc, a.appeal_response_due_date asc  
 end  
else if @data_filter_definitions_id = 16     --Recon/reopen/rereview validation  
 begin  
  if (@appeal_type_id  = '' or @appeal_type_id is null)   
  --We are selecting all the appeals status codes valid for that data filter.  
  insert into #appeal_history_summary_filtered
   select @car_name car_name,  
     a.appeal_history_id,  
     a.auth_id,  
     a.appeal_type_id,  
     b.appeal_type_description,  
     c.appeal_contact_description,  
     dbo.initcap(e.lname + ', ' + e.fname) member_name,  
     dbo.initcap(g.lname + ', ' + g.fname) appeal_physician_name,   
     d.proc_desc,  
     a.appeal_received_date,  
     a.appeal_response_due_date,  
     dbo.initcap(f.lname + ', ' + f.fname) reviewing_physician_name,     
     a.expedited_appeal_flag,  
     h.appeal_status_description status_desc,  
     a.authorization_type_id,  
     d.tracking_number,  
     j.appeal_status_description old_appeal_status_desc,  
     dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id) date_changed, 
	 datediff(hour,dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id),getdate()) hours_in_queue,
     e.dob,   --4/04/2013 Fn  
     d.retro_flag,  --4/04/2013 Fn  
     d.cad_program_flag,  --4/04/2013 Fn  
     d.expedite_flag,  --4/04/2013 Fn  
        d.contact_type_id,  --4/04/2013 Fn  
        null line_of_business,     --5/22/2013 Fn  
        null fax_status_id,        --5/22/2013 Fn  
        null md_callout_status_id,  --5/22/2013 Fn  
     --VJ changes for tkt#199715   
     case when exists (select 1 from member_consenter (nolock) where member_id = e.member_id) then 1  
          else 0  
          end as has_member_representative,  
     dbo.uf_get_project(e.plan_id, 373) has_special_timeliness_requirement, --03/27/18 UMT-82  
     a.external_review_sent_date,  
     a.external_review_determination_date,  
     a.appeal_extended,  
     a.extend_notification_complete,
	 a.expedited_review_requested  
   from appeal_history a (nolock)  
     inner join niacore..appeal_types b (nolock) on a.appeal_type_id = b.appeal_type_id  
     inner join niacore..appeal_contacts c (nolock) on a.appeal_contact_id = c.appeal_contact_id  
     inner join authorizations d (nolock) on a.auth_id = d.auth_id  
     inner join members e (nolock) on d.member_id = e.member_id  
     left outer join niacore..is_users f (nolock) on a.reviewing_physician_id = f.is_user_id   
     inner join physicians g (nolock) on d.phys_id = g.phys_id  
     inner join niacore..appeal_status_codes h (nolock) on a.appeal_status = h.appeal_status  
     inner join appeal_status_change i on a.appeal_history_id = i.appeal_history_id    --Fnoll 7/05/2012   
     inner join niacore..appeal_status_codes j (nolock) on i.old_appeal_status = j.appeal_status         --Fnoll 7/05/2012   
   where a.appeal_status in (select a.assignment  
         from niacore..data_filter_assignments a (nolock),  
           niacore..data_filter_definitions b (nolock),  
           niacore..data_filter_clients c (nolock)  
         where a.data_filter_definitions_id = b.data_filter_definitions_id  
         and b.data_filter_definitions_id = @data_filter_definitions_id   
         and b.data_filter_definitions_id = c.data_filter_definition_id   
         and c.car_id = @car_id)  
         and i.isfinal = 1  
         and a.second_level_denial_verified = 0   --fn 1/27/2014          
         and dbo.uf_get_project(e.plan_id, 161) = 1 --BSHBBI-4345 SG
		 and dbo.uf_get_project(e.plan_id, 216) = 1 --BSHBBI-4345 SG
                -- NG hide auths for the carriers terminated for specific auth type id.  
     and  not exists (select 'true' from niacore..vw_auth_type_terminated_plans   
            where plan_id = e.plan_id  
            and authorization_type_id = a.authorization_type_id)               
   order by a.expedited_appeal_flag desc, a.appeal_response_due_date asc  
 else  
 --We are selecting all the appeals status codes valid for that data filter and also the APPEAL_TYPE. 
 insert into #appeal_history_summary_filtered 
  select @car_name car_name,  
    a.appeal_history_id,  
    a.auth_id,  
    a.appeal_type_id,  
    b.appeal_type_description,  
    c.appeal_contact_description,  
    dbo.initcap(e.lname + ', ' + e.fname) member_name,  
    dbo.initcap(g.lname + ', ' + g.fname) appeal_physician_name,   
    d.proc_desc,  
    a.appeal_received_date,  
    a.appeal_response_due_date,  
    dbo.initcap(f.lname + ', ' + f.fname) reviewing_physician_name,     
    a.expedited_appeal_flag,  
    h.appeal_status_description status_desc,  
    a.authorization_type_id,  
    d.tracking_number,  
    j.appeal_status_description old_appeal_status_desc,  
    dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id) date_changed,  
	datediff(hour,dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id),getdate()) hours_in_queue,
    e.dob,   --4/04/2013 Fn  
    d.retro_flag,  --4/04/2013 Fn  
    d.cad_program_flag,  --4/04/2013 Fn  
    d.expedite_flag,  --4/04/2013 Fn  
       d.contact_type_id,  --4/04/2013 Fn  
       null line_of_business,     --5/22/2013 Fn  
       null fax_status_id,        --5/22/2013 Fn  
       null md_callout_status_id,  --5/22/2013 Fn  
    --VJ changes for tkt#199715   
     case when exists (select 1 from member_consenter (nolock) where member_id = e.member_id) then 1  
          else 0  
          end as has_member_representative,  
    dbo.uf_get_project(e.plan_id, 373) has_special_timeliness_requirement, --03/27/18 UMT-82  
    a.external_review_sent_date,  
    a.external_review_determination_date,  
    a.appeal_extended,  
    a.extend_notification_complete,
	a.expedited_review_requested  
  from appeal_history a (nolock)  
    inner join niacore..appeal_types b (nolock) on a.appeal_type_id = b.appeal_type_id  
    inner join niacore..appeal_contacts c (nolock) on a.appeal_contact_id = c.appeal_contact_id  
    inner join authorizations d (nolock) on a.auth_id = d.auth_id  
    inner join members e (nolock) on d.member_id = e.member_id  
    left outer join niacore..is_users f (nolock) on a.reviewing_physician_id = f.is_user_id   
    inner join physicians g (nolock) on d.phys_id = g.phys_id  
    inner join niacore..appeal_status_codes h (nolock) on a.appeal_status = h.appeal_status  
    inner join appeal_status_change i on a.appeal_history_id = i.appeal_history_id     --Fnoll 7/05/2012   
    inner join niacore..appeal_status_codes j (nolock) on i.old_appeal_status = j.appeal_status         --Fnoll 7/05/2012   
  where a.appeal_status in (select a.assignment  
        from niacore..data_filter_assignments a (nolock) ,  
          niacore..data_filter_definitions b (nolock) ,  
          niacore..data_filter_clients c (nolock)   
        where a.data_filter_definitions_id = b.data_filter_definitions_id  
        and b.data_filter_definitions_id = @data_filter_definitions_id  
        and b.data_filter_definitions_id = c.data_filter_definition_id   
        and c.car_id = @car_id)  
        and a.appeal_type_id = @appeal_type_id  
         and i.isfinal = 1  
        and a.second_level_denial_verified = 0   --fn 1/27/2014            
        and dbo.uf_get_project(e.plan_id, 161) = 1 --BSHBBI-4345 SG
		 and dbo.uf_get_project(e.plan_id, 216) = 1  --BSHBBI-4345 SG
       -- NG hide auths for the carriers terminated for specific auth type id.  
        and  not exists (select 'true' from niacore..vw_auth_type_terminated_plans   
            where plan_id = e.plan_id  
            and authorization_type_id = a.authorization_type_id)             
  order by  a.expedited_appeal_flag desc, a.appeal_response_due_date asc  
 end  
 -- 2/27/2023 bb data_filter_definitions_id = 19 is UMA Queue 
 -- plus data_filter_definitions_id in (2, 8, 9, 17) BSHBBI-4536
else if @data_filter_definitions_id = 19
begin
insert into #appeal_history_summary_filtered
select @car_name as car_name,  
     isnull(ah.appeal_history_id, 0) as appeal_history_id,  
     a.auth_id,  
     ah.appeal_type_id,
	 isnull(apt.appeal_type_description, 'UM Admin Q') as appeal_type_description,  
     c.appeal_contact_description, 
     dbo.initcap(m.lname + ', ' + m.fname) as member_name,  
     dbo.initcap(p.lname + ', ' + p.fname) as appeal_physician_name,   
     a.proc_desc,
	 ah.appeal_received_date,
	 ah.appeal_response_due_date,
	 dbo.initcap(us.lname + ', ' + us.fname) as reviewing_physician_name,
     ah.expedited_appeal_flag,  
     apc.appeal_status_description as status_desc,  
     a.authorization_type_id,  
     a.tracking_number,
	 dbo.uf_get_previous_appeal_status_description(ah.appeal_history_id, ah.appeal_status) as old_appeal_status_desc,
	 dbo.uf_get_appeal_date_changed(ah.appeal_history_id, ah.appeal_status) as date_changed,
	 isnull(datediff(hour, dbo.uf_get_appeal_date_changed(ah.appeal_history_id, ah.appeal_status), 
		sysdatetime()), datediff(hour, dbo.uf_get_last_date_queued(a.auth_id),
		sysdatetime())) as hours_in_queue,
	 m.dob,
	 a.retro_flag,
	 a.cad_program_flag,
	 a.expedite_flag,
	 a.contact_type_id,
     null line_of_business,     --5/22/2013 Fn  
     null fax_status_id,        --5/22/2013 Fn  
     null md_callout_status_id,  --5/22/2013 Fn  
     case when exists (select 1 from member_consenter with (nolock) where member_id = m.member_id) then 1  
          else 0  
          end as has_member_representative,
	 dbo.uf_get_project(m.plan_id, 373) has_special_timeliness_requirement,
     ah.external_review_sent_date,  
     ah.external_review_determination_date,  
     ah.appeal_extended,  
     ah.extend_notification_complete,
	 ah.expedited_review_requested
 from authorizations a with (nolock)
 join members m with (nolock) on a.member_id = m.member_id
 join physicians p (nolock) on a.phys_id = p.phys_id
 left join appeal_history ah with (nolock) on a.auth_id = ah.auth_id
 left join niacore..appeal_types apt with (nolock) on ah.appeal_type_id = apt.appeal_type_id
 left join niacore..appeal_contacts c with (nolock) on ah.appeal_contact_id = c.appeal_contact_id
 left join niacore..is_users us with (nolock) on ah.reviewing_physician_id = us.is_user_id
 left join niacore..appeal_status_codes apc with (nolock) on ah.appeal_status = apc.appeal_status
 where dbo.uf_select_auth_isfinal_queue (a.auth_id) = 'UM Admin Review Queue'
 and not exists (select 'true' from niacore..vw_auth_type_terminated_plans   
            where plan_id = m.plan_id  
            and authorization_type_id = a.authorization_type_id)  

-- everything in UMA plus requests in these appeal queues: Coordinator/Triage Queue, 
-- Appeals Information Callback Queue, Appeals Notifications Queue and ICR Appeal Review Queue
union
	select @car_name as car_name,   
     ah.appeal_history_id,  
     a.auth_id,  
     ah.appeal_type_id,
	 apt.appeal_type_description,  
     c.appeal_contact_description, 
     dbo.initcap(m.lname + ', ' + m.fname) as member_name,  
     dbo.initcap(p.lname + ', ' + p.fname) as appeal_physician_name,   
     a.proc_desc,
	 ah.appeal_received_date,
	 ah.appeal_response_due_date,
	 dbo.initcap(us.lname + ', ' + us.fname) as reviewing_physician_name,
     ah.expedited_appeal_flag,  
     apc.appeal_status_description as status_desc,  
     a.authorization_type_id,  
     a.tracking_number,
	 dbo.uf_get_previous_appeal_status_description(ah.appeal_history_id, ah.appeal_status) as old_appeal_status_desc,
	 dbo.uf_get_appeal_date_changed(ah.appeal_history_id, ah.appeal_status) as date_changed,
	 datediff(hour, dbo.uf_get_appeal_date_changed(ah.appeal_history_id, ah.appeal_status), 
		sysdatetime()) as hours_in_queue,
	 m.dob,
	 a.retro_flag,
	 a.cad_program_flag,
	 a.expedite_flag,
	 a.contact_type_id,
     null line_of_business,     --5/22/2013 Fn  
     null fax_status_id,        --5/22/2013 Fn  
     null md_callout_status_id,  --5/22/2013 Fn  
     case when exists (select 1 from member_consenter with (nolock) where member_id = m.member_id) then 1  
          else 0  
          end as has_member_representative,
	 dbo.uf_get_project(m.plan_id, 373) has_special_timeliness_requirement,
     ah.external_review_sent_date,  
     ah.external_review_determination_date,  
     ah.appeal_extended,  
     ah.extend_notification_complete,
	 ah.expedited_review_requested
 from authorizations a with (nolock)
 join members m with (nolock) on a.member_id = m.member_id
 join physicians p (nolock) on a.phys_id = p.phys_id
 join appeal_history ah with (nolock) on a.auth_id = ah.auth_id
 join niacore..appeal_types apt with (nolock) on ah.appeal_type_id = apt.appeal_type_id
 join niacore..appeal_contacts c with (nolock) on ah.appeal_contact_id = c.appeal_contact_id
 join niacore..is_users us with (nolock) on ah.reviewing_physician_id = us.is_user_id
 join niacore..appeal_status_codes apc with (nolock) on ah.appeal_status = apc.appeal_status
 join niacore..data_filter_assignments fa with (nolock) on ah.appeal_status = fa.assignment
 join niacore..data_filter_definitions fd with (nolock) 
	on fa.data_filter_definitions_id = fd.data_filter_definitions_id
 join niacore..data_filter_clients dfc with (nolock) 
	on fa.data_filter_definitions_id = dfc.data_filter_definition_id
 where dfc.car_id = @car_id
 ---and fd.data_filter_definitions_id in (2, 8, 9, 17)
  and fd.data_filter_definitions_id in (2, 8, 9) -- NG removing 17 BSHBBI-5977
 and not exists (select 'true' from niacore..vw_auth_type_terminated_plans   
            where plan_id = m.plan_id  
            and authorization_type_id = a.authorization_type_id);
end
else  
 begin  
  if (@appeal_type_id  = '' or @appeal_type_id is null)   
  --We are selecting all the appeals status codes valid for that data filter.  
  insert into #appeal_history_summary_filtered
   select @car_name car_name,  
     a.appeal_history_id,  
     a.auth_id,  
     a.appeal_type_id,  
     b.appeal_type_description,  
     c.appeal_contact_description,  
     dbo.initcap(e.lname + ', ' + e.fname) member_name,  
     dbo.initcap(g.lname + ', ' + g.fname) appeal_physician_name,   
     d.proc_desc,  
     a.appeal_received_date,  
     a.appeal_response_due_date,  
     dbo.initcap(f.lname + ', ' + f.fname) reviewing_physician_name,     
     a.expedited_appeal_flag,  
     h.appeal_status_description status_desc,  
     a.authorization_type_id,  
     d.tracking_number,  
     j.appeal_status_description old_appeal_status_desc,  
     dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id) date_changed,  
	 datediff(hour,dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id),getdate()) hours_in_queue,
     e.dob,   --4/04/2013 Fn  
     d.retro_flag,  --4/04/2013 Fn  
     d.cad_program_flag,  --4/04/2013 Fn  
     d.expedite_flag,  --4/04/2013 Fn  
        d.contact_type_id,  --4/04/2013 Fn  
        null line_of_business,     --5/22/2013 Fn  
        null fax_status_id,        --5/22/2013 Fn  
        null md_callout_status_id,  --5/22/2013 Fn  
--     k.line_of_business,  --4/04/2013 Fn  
--     l.fax_status_id,  --4/04/2013 Fn  
--     l.md_callout_status_id --4/04/2013 Fn  
--VJ changes for tkt#199715   
     case when exists (select 1 from member_consenter (nolock) where member_id = e.member_id) then 1  
          else 0  
          end as has_member_representative,  
     dbo.uf_get_project(e.plan_id, 373) has_special_timeliness_requirement, --03/27/18 UMT-82  
     a.external_review_sent_date,  
     a.external_review_determination_date,  
     a.appeal_extended,  
     a.extend_notification_complete,
	 a.expedited_review_requested  
   from appeal_history a (nolock)  
     inner join niacore..appeal_types b (nolock) on a.appeal_type_id = b.appeal_type_id  
     inner join niacore..appeal_contacts c (nolock) on a.appeal_contact_id = c.appeal_contact_id  
     inner join authorizations d (nolock) on a.auth_id = d.auth_id  
     inner join members e (nolock) on d.member_id = e.member_id  
     left outer join niacore..is_users f (nolock) on a.reviewing_physician_id = f.is_user_id   
     inner join physicians g (nolock) on d.phys_id = g.phys_id  
     inner join niacore..appeal_status_codes h (nolock) on a.appeal_status = h.appeal_status  
     inner join appeal_status_change i on a.appeal_history_id = i.appeal_history_id    --Fnoll 7/05/2012   
     inner join niacore..appeal_status_codes j (nolock) on i.old_appeal_status = j.appeal_status         --Fnoll 7/05/2012   
--     inner join niacore..appeal_info k (nolock) on a.appeal_info_id = k.appeal_info_id                   --Fnoll 4/04/2013   
--     left outer join auth_queue_values l (nolock) on d.auth_id = l.auth_id                               --Fnoll 4/04/2013   
   where a.appeal_status in (select a.assignment  
         from niacore..data_filter_assignments a (nolock),  
           niacore..data_filter_definitions b (nolock),  
           niacore..data_filter_clients c (nolock)  
         where a.data_filter_definitions_id = b.data_filter_definitions_id  
         and b.data_filter_definitions_id = @data_filter_definitions_id   
         and b.data_filter_definitions_id = c.data_filter_definition_id   
         and c.car_id = @car_id)  
--       and a.authorization_type_id = @authorization_type_id  
       and i.isfinal = 1               
               -- NG hide auths for the carriers terminated for specific auth type id.  
     and  not exists (select 'true' from niacore..vw_auth_type_terminated_plans   
            where plan_id = e.plan_id  
            and authorization_type_id = a.authorization_type_id)             
   order by a.expedited_appeal_flag desc, a.appeal_response_due_date asc  
 else  
 --We are selecting all the appeals status codes valid for that data filter and also the APPEAL_TYPE. 
 insert into #appeal_history_summary_filtered 
  select @car_name car_name,  
    a.appeal_history_id,  
    a.auth_id,  
    a.appeal_type_id,  
    b.appeal_type_description,  
    c.appeal_contact_description,  
    dbo.initcap(e.lname + ', ' + e.fname) member_name,  
    dbo.initcap(g.lname + ', ' + g.fname) appeal_physician_name,   
    d.proc_desc,  
    a.appeal_received_date,  
    a.appeal_response_due_date,  
    dbo.initcap(f.lname + ', ' + f.fname) reviewing_physician_name,     
    a.expedited_appeal_flag,  
    h.appeal_status_description status_desc,  
    a.authorization_type_id,  
    d.tracking_number,  
    j.appeal_status_description old_appeal_status_desc,  
    dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id) date_changed,  
	datediff(hour,dbo.uf_get_appeal_history_date_changed(a.appeal_history_id, @data_filter_definitions_id),getdate()) hours_in_queue,
    e.dob,   --4/04/2013 Fn  
    d.retro_flag,  --4/04/2013 Fn  
    d.cad_program_flag,  --4/04/2013 Fn  
    d.expedite_flag,  --4/04/2013 Fn  
       d.contact_type_id,  --4/04/2013 Fn  
       null line_of_business,     --5/22/2013 Fn  
       null fax_status_id,        --5/22/2013 Fn  
       null md_callout_status_id,  --5/22/2013 Fn  
--    k.line_of_business,  --4/04/2013 Fn  
--    l.fax_status_id,  --4/04/2013 Fn  
--    l.md_callout_status_id --4/04/2013 Fn  
--VJ changes for tkt#199715   
     case when exists (select 1 from member_consenter (nolock) where member_id = e.member_id) then 1  
          else 0  
          end as has_member_representative,  
     dbo.uf_get_project(e.plan_id, 373) has_special_timeliness_requirement, --03/27/18 UMT-82  
    a.external_review_sent_date,  
    a.external_review_determination_date,  
    a.appeal_extended,  
    a.extend_notification_complete,
	a.expedited_review_requested  
  from appeal_history a (nolock)  
    inner join niacore..appeal_types b (nolock) on a.appeal_type_id = b.appeal_type_id  
    inner join niacore..appeal_contacts c (nolock) on a.appeal_contact_id = c.appeal_contact_id  
    inner join authorizations d (nolock) on a.auth_id = d.auth_id  
    inner join members e (nolock) on d.member_id = e.member_id  
    left outer join niacore..is_users f (nolock) on a.reviewing_physician_id = f.is_user_id   
    inner join physicians g (nolock) on d.phys_id = g.phys_id  
    inner join niacore..appeal_status_codes h (nolock) on a.appeal_status = h.appeal_status  
    inner join appeal_status_change i on a.appeal_history_id = i.appeal_history_id     --Fnoll 7/05/2012   
    inner join niacore..appeal_status_codes j (nolock) on i.old_appeal_status = j.appeal_status         --Fnoll 7/05/2012   
--    inner join niacore..appeal_info k (nolock) on a.appeal_info_id = k.appeal_info_id                   --Fnoll 4/04/2013   
--    left outer join auth_queue_values l (nolock) on d.auth_id = l.auth_id                               --Fnoll 4/04/2013   
  where a.appeal_status in (select a.assignment  
        from niacore..data_filter_assignments a (nolock) ,  
          niacore..data_filter_definitions b (nolock) ,  
          niacore..data_filter_clients c (nolock)   
        where a.data_filter_definitions_id = b.data_filter_definitions_id  
        and b.data_filter_definitions_id = @data_filter_definitions_id  
        and b.data_filter_definitions_id = c.data_filter_definition_id   
        and c.car_id = @car_id)  
    and a.appeal_type_id = @appeal_type_id  
--    and a.authorization_type_id = @authorization_type_id  
        and i.isfinal = 1                   
            -- NG hide auths for the carriers terminated for specific auth type id.  
    and  not exists (select 'true' from niacore..vw_auth_type_terminated_plans   
            where plan_id = e.plan_id  
            and authorization_type_id = a.authorization_type_id)  
  order by  a.expedited_appeal_flag desc, a.appeal_response_due_date asc  
 end  
end  


insert into #auth_date_of_last_doc_rcvd
select ahs.auth_id, max(date_entered)
from #appeal_history_summary_filtered ahs
	inner join auth_action_log (nolock) al on ahs.auth_id=al.auth_id
	inner join  niacore..auth_action_codes (nolock) ac on al.auth_action_code=ac.auth_action_code
where clinical_info_rcvd = 1
group by ahs.auth_id

-- NG 09/08/23 calling SP  usp_select_appeal_history_queues which inserts data for each auth belonging to various appeal queues
select  *,
	RowNum = row_number() OVER ( order by auth_id )
into #tmpAppeal2
from   #appeal_history_summary_filtered 

select @cnt= 0
select @cnt = count(*) from #tmpAppeal2
select @counter = 0
while @counter < @cnt
	begin
	select @appeal_auth_id = auth_id from #tmpAppeal2
	where RowNum = @counter
	exec usp_select_appeal_history_queues @appeal_auth_id
	select @counter = @counter+1
end

IF OBJECT_ID('tempdb..#tmpAppeal2') IS NOT NULL
drop table #tmpAppeal2

select a.*,
FORMAT(adt.dt_of_last_doc_rcvd,'M/d/yyyy h:mm:ss tt') as dt_of_last_doc_rcvd,
auth_queue =  dbo.uf_select_auth_isfinal_queue (a.auth_id),
auth_appeal_queues = dbo.udf_get_appeal_history_queues(appeal_history_id,@car_id)
into #appeal_history_summary_final
from #appeal_history_summary_filtered a 
left join #auth_date_of_last_doc_rcvd adt on a.auth_id = adt.auth_id


if @SortOrder is null or @SortOrder = ''
select @SortOrder = 'expedited_appeal_flag desc, appeal_response_due_date asc '


declare @msql varchar(max)
	 set @msql =
'select ahs.* from #appeal_history_summary_final ahs
order by  '+@SortOrder

EXEC(@msql)

Drop Table #appeal_history_summary_filtered
Drop Table #auth_date_of_last_doc_rcvd
drop table #appeal_history_summary_final
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
drop table #appeal_history_global_link
drop table #appeal_history


end


GO
GRANT EXECUTE ON [dbo].[usp_select_appeal_history_summary_filtered] TO [db_execallsp]
GO
GRANT EXECUTE ON [dbo].[usp_select_appeal_history_summary_filtered] TO [informa_admin]
GO
GRANT EXECUTE ON [dbo].[usp_select_appeal_history_summary_filtered] TO [informa_users]
GO
GRANT EXECUTE ON [dbo].[usp_select_appeal_history_summary_filtered] TO [sf_it]
GO
GO
GRANT EXECUTE ON [dbo].[usp_select_appeal_history_summary_global_filtered] TO [db_execallsp]
GO
GRANT EXECUTE ON [dbo].[usp_select_appeal_history_summary_global_filtered] TO [informa_admin]
GO
GRANT EXECUTE ON [dbo].[usp_select_appeal_history_summary_global_filtered] TO [informa_users]
GO
GRANT EXECUTE ON [dbo].[usp_select_appeal_history_summary_global_filtered] TO [sf_it]