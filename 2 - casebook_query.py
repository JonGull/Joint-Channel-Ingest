import _sql_access_utils
import pandas as pd
import re
import time
from _joint_record_helpers import *

start_time = time.time()

# Generate a blank list of entries from our mapping tab for every date. 
# Our chat logs span back to '2020-06-01' earliest.

starting_date = '2022-02-07'

# Pull SQL views for chat, email, phone.


email_query = f'''
select 

1 as number_of_emails,
(CASE 
WHEN DATEDIFF(day, CAST(tasks.created_at AS DATE), CAST(closed_at AS DATE)) 
	- (datediff(wk, CAST(tasks.created_at AS DATE), CAST(closed_at AS DATE)) * 2)  <= 4
THEN 1
ELSE 
0 END) as responses_under_4_days,
(CASE 
WHEN DATEDIFF(day, CAST(tasks.created_at AS DATE), CAST(closed_at AS DATE))
- (datediff(wk, CAST(tasks.created_at AS DATE), CAST(closed_at AS DATE)) * 2) <= 2
THEN 1
ELSE 
0 END) as responses_under_2_days,  
CAST(closed_at AS DATE) as closed_date,
locations1.member_number from tasks
left join task_lists on tasks.task_list_id=task_lists.id
left join users assignee on tasks.user_id=assignee.id
left join locations locations on assignee.location_id=locations.id
left join locations locations1 on locations.office_group_id=locations1.id
where task_lists.title = 'National - Email task list' and task_status = 'completed' 
'''
email_df = _sql_access_utils.get_query_as_dataframe(query=email_query ,database='Casebook',tableau_server=True)
email_df.closed_date = pd.to_datetime(email_df.closed_date)

#df = pd.DataFrame(rows_list)

#output_df = df[['PK_ID','answered_date', 'lca', 'member_number', 
#'no_of_inbound_calls', 'no_of_outbound_calls', 'no_of_emails', 'no_of_chats',]]

#output_df = df.sort_values(by=['answered_date'], ascending = True)
email_df = email_df.dropna()
email_df = email_df[email_df['member_number'] != '0']
email_df.to_csv('email_table.csv', index=False)

#print(f"All done for records beginning {starting_date}! {start_time} to {time.time()}: {int(time.time() - start_time)} seconds, {(time.time() - start_time)/60} minutes.")

#print(f"Phone query took {(phone_query_end_time - phone_query_start_time)/60} minutes to complete.")