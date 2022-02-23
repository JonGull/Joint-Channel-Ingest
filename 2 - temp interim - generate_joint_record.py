import _sql_access_utils
import pandas as pd
import re
import time
from _joint_record_helpers import *

start_time = time.time()

# Generate a blank list of entries from our mapping tab for every date. 
# Our chat logs span back to '2020-06-01' earliest.

starting_date = '2020-01-01'
get_calendar_date_table_query = f'''Select * from [dbo].[udf-Range-Date]('{starting_date}', DATEADD(d, -1, GETDATE()),'DD',1) '''
calendar_df = _sql_access_utils.get_query_as_dataframe(query=get_calendar_date_table_query ,database='kcom_data')

# Pull SQL views for chat, email, phone.

lca_joint_list_query = '''select * from dim_AL_chat_email_phone_lookup'''
lca_lookup = _sql_access_utils.get_query_as_dataframe(query=lca_joint_list_query ,database='liveengage_chat_data')

chat_df = _sql_access_utils.get_query_as_dataframe(query=f'select * from dbo.view_chat_data_general_aggregated' ,database='liveengage_chat_data')
chat_df.chat_date = pd.to_datetime(chat_df.chat_date).dt.date

phone_query_start_time = time.time()
query_phone = f'''select * from dbo.view_adviceline_phone_dashboard_aggregated_no_agents
where answered_date >= '{starting_date}'
'''
phone_df = _sql_access_utils.get_query_as_dataframe(query=query_phone ,database='kcom_data')
phone_df.answered_date = pd.to_datetime(phone_df.answered_date).dt.date
# Temporary workaround - Newport/Cardiff/Flintshire were registering extra calls because the employment & discrimination and private tenant specialists were sharing their numbers. I'm removing these specialists with this logic.
welsh_specialist_groups = ['Employment and Discrimination Specialist', 'Cardiff Private Sector Tenant line']
phone_df = phone_df[~phone_df['answered_member'].isin(welsh_specialist_groups)]

phone_query_end_time = time.time()

email_query = f'''
select count (distinct tasks.id) as number_of_emails,
SUM((CASE 
WHEN DATEDIFF(day, CAST(tasks.created_at AS DATE), CAST(closed_at AS DATE)) 
	- (datediff(wk, CAST(tasks.created_at AS DATE), CAST(closed_at AS DATE)) * 2)  <= 4
THEN 1
ELSE 
0 END)) as responses_under_4_days,
SUM((CASE 
WHEN DATEDIFF(day, CAST(tasks.created_at AS DATE), CAST(closed_at AS DATE))
- (datediff(wk, CAST(tasks.created_at AS DATE), CAST(closed_at AS DATE)) * 2) <= 2
THEN 1
ELSE 
0 END)) as responses_under_2_days,  
CAST(closed_at AS DATE) as closed_date,
locations.name, 
locations1.member_number from tasks
left join task_lists on tasks.task_list_id=task_lists.id
left join users assignee on tasks.user_id=assignee.id
left join locations locations on assignee.location_id=locations.id
left join locations locations1 on locations.office_group_id=locations1.id
where task_lists.title = 'National - Email task list' and task_status = 'completed' 
group by  CAST(closed_at AS DATE),locations.name, locations1.member_number
'''
email_df = _sql_access_utils.get_query_as_dataframe(query=email_query ,database='Casebook',tableau_server=True)
email_df.closed_date = pd.to_datetime(email_df.closed_date)

logtables_df = _sql_access_utils.get_query_as_dataframe(query=f'select * from LogTables.lcamembers_SC' ,database='kcom_data')

rows_list = []

for day in calendar_df.RetVal:
    if '-01 ' in str(day):
        print(f'Processing records for {str(day)}.')
    for index, row in lca_lookup.iterrows():
        lca = row['lca']
        member_number = row['member_number']
        inbound_within_office_hours_short_duration_calls, inbound_within_office_hours_long_duration_calls, inbound_outside_office_hours_short_duration_calls, inbound_outside_office_hours_long_duration_calls, outbound_within_office_hours_short_duration_calls, outbound_within_office_hours_long_duration_calls, outbound_outside_office_hours_short_duration_calls, outbound_outside_office_hours_long_duration_calls = phone_data_lookup(phone_df, member_number, day.date())
        inbound = inbound_within_office_hours_long_duration_calls
        outbound = outbound_within_office_hours_long_duration_calls
        chat_dict = chat_data_lookup(chat_df, lca, day.date())
        adviceline_group = lookup_adviceline_group(member_number, lca, day, logtables_df)
        no_of_emails, responded_in_4_days, responded_in_2_days = email_data_lookup(email_df, member_number, day)
        date_entry = {
        "PK_ID":'',
        "answered_date":day,
        "lca":lca,
        "adviceline_group":adviceline_group,
        "member_number":member_number,
        "no_of_inbound_calls": int(inbound),
        "no_of_outbound_calls": int(outbound),
        "no_of_emails": no_of_emails,
        "responded_in_four_days": responded_in_4_days,
        "responded_in_two_days": responded_in_2_days,
        "no_of_chats": chat_dict['chats'],
        "chat_surveys_answered": chat_dict['post_chat_surveys'],
        "chat_csat_very_good": chat_dict['csat_very_good'],
        "chat_csat_fairly_good": chat_dict['csat_fairly_good'],
        "chat_csat_neither_good_nor_poor": chat_dict['csat_neither_good_nor_poor'],
        "chat_csat_fairly_poor": chat_dict['csat_fairly_poor'],
        "chat_csat_very_poor": chat_dict['csat_very_poor'],
        "chat_csat_total_satisfied": chat_dict['csat_total_satisfied']   
        }
        rows_list.append(date_entry)

df = pd.DataFrame(rows_list)

#output_df = df[['PK_ID','answered_date', 'lca', 'member_number', 
#'no_of_inbound_calls', 'no_of_outbound_calls', 'no_of_emails', 'no_of_chats',]]

output_df = df.sort_values(by=['answered_date'], ascending = True)

output_df.to_csv('joint_table.csv', index=False)

print(f"All done for records beginning {starting_date}! {start_time} to {time.time()}: {int(time.time() - start_time)} seconds, {(time.time() - start_time)/60} minutes.")

print(f"Phone query took {(phone_query_end_time - phone_query_start_time)/60} minutes to complete.")