import pandas as pd
import json
import _load_utils
import _sql_access_utils

# This script takes all the files in the Ingests folder and converts them into a single file for ingest.
# This output file can be found in the Output folder.

# The different pre- and post- chat surveys for different skills get combined into one field.

# Get all files in our folder

from os import listdir
from os.path import isfile, join
path_to_folder = 'Ingests/'
path_to_file = [f for f in listdir(path_to_folder) if isfile(join(path_to_folder, f))]

csv_df = pd.DataFrame(columns=['pk_id','Start (UTC)', 'Visitor ID', 'Visitor Name', 'MCS',  'Skill', 'Engagement ID',
       'Agent Name', 'Agent Login Name', 'Agent Full Name', 'Agent Group',
       'Chat Start Reason', 'Chat End Reason', 'Chat Requested Time (UTC)',
       'Length (seconds)', 'Campaign',
       'Target Audience', 'Engagement Name', 'Country',
       'State', 'City', 'ISP', 'Organization', 'IP Address', 'Device',
       'Browser', 'Operating System', 'Chat Start Page', 'Chat Start URL', 'Pre-Chat Survey Exists', 'Post-Chat Survey Exists',
       'Pre-Chat Survey - Pre-chat survey CitA - What do you need advice about?',
       'Pre-Chat Survey - Pre-chat survey CitA - What do you need advice about?.1',
       'Pre-Chat Survey - Pre-chat survey CitA - Your name',
       'Pre-Chat Survey - Pre-chat survey CitA - Your postcode',
       'Exit Survey - Post-chat survey - Rate your overall experience',
       'Exit Survey - Post-chat survey - How easy or difficult was it to use the chat service?',
       'Exit Survey - Post-chat survey - Did the chat service help you find a way forward?',
       'Exit Survey - Post-chat survey - Is your problem now resolved?',
       'Exit Survey - Post-chat survey - Would you recommend this service to friends or family?',
       'Pre-Chat Survey - Consumer trading standards - Please provide your home postcode if you have one',
 'Pre-Chat Survey - Consumer trading standards - \r\nPlease provide your home postcode if you have one',
 'Pre-Chat Survey - Debt pre chat survey - Please provide your home postcode if you have one',
 'Pre-Chat Survey - Debt pre chat survey - First part of your postcode (eg WD23)',
 'Pre-Chat Survey - General training - Please provide your home postcode if you have one',
 'Pre-Chat Survey - General training - Please provide your home postcode if you have one.1',
 'Pre-Chat Survey - NI Debt Chat - Your postcode',
 'Pre-Chat Survey - Pre-chat survey - Please provide your home postcode if you have one',
 'Pre-Chat Survey - Pre-chat survey CitA - What is your postcode?',
 'Pre-Chat Survey - Scams pre-chat survey - Please provide your home postcode if you have one\r\n',
 'Pre-Chat Survey - Scams pre-chat survey - Please provide your home postcode if you have one\r\n.1',
 'Pre-Chat Survey - Universal Support - Please provide you home postcode if you have one',
 'Pre-Chat Survey - Universal Support - Postcode',
 'Pre-Chat Survey - Universal Support - Your postcode',
 'Pre-Chat Survey - Consumer trading standards - Postcode (if you have one)'
])

for path in path_to_file:
    tmp_csv_df = pd.read_csv(path_to_folder + path, skiprows=8)
    tmp_csv_df['Start (UTC)'] = pd.to_datetime(tmp_csv_df['Start (UTC)']).values.astype('<M8[ns]')
    tmp_csv_df['Chat Requested Time (UTC)'] = pd.to_datetime(tmp_csv_df['Chat Requested Time (UTC)']).values.astype('<M8[ns]')
    tmp_csv_df.insert(0,'pk_id','')
    print(len(tmp_csv_df))
    csv_df = csv_df.append(tmp_csv_df)
    print(len(csv_df))

main_ingest = csv_df[['pk_id','Start (UTC)', 'Visitor ID', 'Visitor Name', 'MCS',  'Skill', 'Engagement ID',
       'Agent Name', 'Agent Login Name', 'Agent Full Name', 'Agent Group',
       'Chat Start Reason', 'Chat End Reason', 'Chat Requested Time (UTC)',
       'Length (seconds)', 'Campaign',
       'Target Audience', 'Engagement Name', 'Country',
       'State', 'City', 'ISP', 'Organization', 'IP Address', 'Device',
       'Browser', 'Operating System', 'Chat Start Page', 'Chat Start URL', 'Pre-Chat Survey Exists', 'Post-Chat Survey Exists',
        'Pre-Chat Survey - Pre-chat survey CitA - What do you need advice about?',
       'Pre-Chat Survey - Pre-chat survey CitA - Your name',
       'Pre-Chat Survey - Pre-chat survey CitA - Your postcode',
       'Exit Survey - Post-chat survey - Rate your overall experience',
       'Exit Survey - Post-chat survey - How easy or difficult was it to use the chat service?',
       'Exit Survey - Post-chat survey - Did the chat service help you find a way forward?',
       'Exit Survey - Post-chat survey - Is your problem now resolved?',
       'Exit Survey - Post-chat survey - Would you recommend this service to friends or family?'
]]

# Merge the Postcode pre-surveys into a single field

main_ingest['Pre-Chat Survey - Pre-chat survey CitA - Your postcode'] = csv_df['Pre-Chat Survey - Pre-chat survey CitA - Your postcode'].str.cat(csv_df[['Pre-Chat Survey - Consumer trading standards - Please provide your home postcode if you have one',
 'Pre-Chat Survey - Consumer trading standards - \r\nPlease provide your home postcode if you have one',
 'Pre-Chat Survey - Debt pre chat survey - Please provide your home postcode if you have one',
 'Pre-Chat Survey - Debt pre chat survey - First part of your postcode (eg WD23)',
 'Pre-Chat Survey - General training - Please provide your home postcode if you have one',
 'Pre-Chat Survey - General training - Please provide your home postcode if you have one.1',
 'Pre-Chat Survey - NI Debt Chat - Your postcode',
 'Pre-Chat Survey - Pre-chat survey - Please provide your home postcode if you have one',
 'Pre-Chat Survey - Pre-chat survey CitA - What is your postcode?',
 'Pre-Chat Survey - Scams pre-chat survey - Please provide your home postcode if you have one\r\n',
 'Pre-Chat Survey - Scams pre-chat survey - Please provide your home postcode if you have one\r\n.1',
 'Pre-Chat Survey - Universal Support - Please provide you home postcode if you have one',
 'Pre-Chat Survey - Universal Support - Postcode',
 'Pre-Chat Survey - Universal Support - Your postcode',
 'Pre-Chat Survey - Consumer trading standards - Postcode (if you have one)']], na_rep='')

# Cast to upper.
main_ingest['Pre-Chat Survey - Pre-chat survey CitA - Your postcode'] = main_ingest['Pre-Chat Survey - Pre-chat survey CitA - Your postcode'].str.upper()

#Reformat the postcode - reduce all spaces, and then separate the final three characters.

main_ingest['Pre-Chat Survey - Pre-chat survey CitA - Your postcode'] = main_ingest['Pre-Chat Survey - Pre-chat survey CitA - Your postcode'].str.replace(' ','')
main_ingest['Pre-Chat Survey - Pre-chat survey CitA - Your postcode'] = main_ingest['Pre-Chat Survey - Pre-chat survey CitA - Your postcode'].str[:-3] + ' ' + main_ingest['Pre-Chat Survey - Pre-chat survey CitA - Your postcode'].str[-3:]

 #Due to an operational error, there's a duplicate of the question "What do you need advice about?". We merge these into one, as they're meant to be the same question.
main_ingest['Pre-Chat Survey - Pre-chat survey CitA - What do you need advice about?'] = csv_df['Pre-Chat Survey - Pre-chat survey CitA - What do you need advice about?.1'].str.cat(csv_df['Pre-Chat Survey - Pre-chat survey CitA - What do you need advice about?'], na_rep='')

       

survey_fields = ['Pre-Chat Survey - Pre-chat survey CitA - What do you need advice about?',
       'Pre-Chat Survey - Pre-chat survey CitA - Your name',
       'Pre-Chat Survey - Pre-chat survey CitA - Your postcode',
       'Exit Survey - Post-chat survey - Rate your overall experience',
       'Exit Survey - Post-chat survey - How easy or difficult was it to use the chat service?',
       'Exit Survey - Post-chat survey - Did the chat service help you find a way forward?',
       'Exit Survey - Post-chat survey - Is your problem now resolved?',
       'Exit Survey - Post-chat survey - Would you recommend this service to friends or family?']

# Truncate surveys.
main_ingest['Pre-Chat Survey - Pre-chat survey CitA - What do you need advice about?'] = main_ingest['Pre-Chat Survey - Pre-chat survey CitA - What do you need advice about?'].astype(str).str.slice(0,354)
main_ingest['Pre-Chat Survey - Pre-chat survey CitA - Your name'] = main_ingest['Pre-Chat Survey - Pre-chat survey CitA - Your name'].astype(str).str.slice(0,99)
main_ingest['Pre-Chat Survey - Pre-chat survey CitA - Your postcode'] = main_ingest['Pre-Chat Survey - Pre-chat survey CitA - Your postcode'].astype(str).str.slice(0,40)
main_ingest['Exit Survey - Post-chat survey - Rate your overall experience'] = main_ingest[ 'Exit Survey - Post-chat survey - Rate your overall experience'].astype(str).str.slice(0,99)
main_ingest['Exit Survey - Post-chat survey - How easy or difficult was it to use the chat service?'] = main_ingest['Exit Survey - Post-chat survey - How easy or difficult was it to use the chat service?'].astype(str).str.slice(0,99)
main_ingest['Exit Survey - Post-chat survey - Did the chat service help you find a way forward?'] = main_ingest['Exit Survey - Post-chat survey - Did the chat service help you find a way forward?'].astype(str).str.slice(0,49)
main_ingest['Exit Survey - Post-chat survey - Is your problem now resolved?'] = main_ingest['Exit Survey - Post-chat survey - Is your problem now resolved?'].astype(str).str.slice(0,49)
main_ingest['Exit Survey - Post-chat survey - Would you recommend this service to friends or family?'] = main_ingest['Exit Survey - Post-chat survey - Would you recommend this service to friends or family?'].astype(str).str.slice(0,49)

for survey_question in survey_fields:
    main_ingest[survey_question] = main_ingest[survey_question].str.replace(r'^nan$', '')

main_ingest = main_ingest.sort_values(by=['Start (UTC)'], ascending = True)

main_ingest.to_csv('./Ingests/Outputs/converted_ingest.tsv', index=False, sep='\t')
dates = main_ingest['Start (UTC)'].to_list()
print(f'Earliest date: {dates[0]}. Latest date: {dates[-1]}')
print('Ingest completed.')

