import _sql_access_utils
import pandas as pd
import re
import time

# The Combiner
def chat_data_lookup(chat_df, lca, day):
    day = pd.to_datetime(day)
    chat_lookup = chat_df[chat_df.chat_date == day]
    lca = lca.split('(')[0].strip() #Remove anything preceding brackets. 
    #For dealing with our temporary merger naming solution - Citizens Advice Buckinghamshire (Aylesbury Vale)
    chat_lookup = chat_lookup[chat_lookup.lca.str.contains(lca, na=False, regex=False)]
    if len(chat_lookup) >= 1:
        #return str(chat_lookup.chats.iloc[0])
    #elif len(chat_lookup) > 1:
        return {"chats": str(chat_lookup.chats.sum()), 
        "post_chat_surveys": str(chat_lookup.post_chat_surveys.sum()),
        "csat_very_good": str(chat_lookup.csat_very_good.sum()),
        "csat_fairly_good": str(chat_lookup.csat_fairly_good.sum()),
        "csat_neither_good_nor_poor": str(chat_lookup.csat_neither_good_nor_poor.sum()),
        "csat_fairly_poor": str(chat_lookup.csat_fairly_poor.sum()),
        "csat_very_poor": str(chat_lookup.csat_very_poor.sum()),
        "csat_total_satisfied": str(chat_lookup.csat_satisfied.sum())}
    else:
        return {"chats": 0, 
        "post_chat_surveys": 0,
        "csat_very_good": 0,
        "csat_fairly_good": 0,
        "csat_neither_good_nor_poor": 0,
        "csat_fairly_poor": 0,
        "csat_very_poor": 0,
        "csat_total_satisfied": 0}
    return "Chat Data Lookup Error"

def phone_data_lookup(phone_df, member_number, day):
    member_number = reassign_gwent_member_numbers(member_number)
    phone_lookup = phone_df[phone_df.answered_date == day]
    phone_lookup = phone_lookup[phone_lookup.member_number.str.fullmatch(member_number, na=False)]
    inbound_lookup = phone_lookup[phone_lookup.outbound_any == False]
    # Within office hours
    inbound_within_office_hours_lookup = inbound_lookup[inbound_lookup.within_office_hours == True]
    inbound_outside_office_hours_lookup = inbound_lookup[inbound_lookup.within_office_hours == False]
        # Short duration
    inbound_within_office_hours_short_duration_lookup = inbound_within_office_hours_lookup[inbound_within_office_hours_lookup.duration_greater_than_31 == False]
    inbound_outside_office_hours_short_duration_lookup = inbound_outside_office_hours_lookup[inbound_outside_office_hours_lookup.duration_greater_than_31 == False]
        # Long duration
    inbound_within_office_hours_long_duration_lookup = inbound_within_office_hours_lookup[inbound_within_office_hours_lookup.duration_greater_than_31 == True]
    inbound_outside_office_hours_long_duration_lookup = inbound_outside_office_hours_lookup[inbound_outside_office_hours_lookup.duration_greater_than_31 == True]

    outbound_lookup = phone_lookup[phone_lookup.outbound_any == True]
     # Within office hours
    outbound_within_office_hours_lookup = outbound_lookup[outbound_lookup.within_office_hours == True]
    outbound_outside_office_hours_lookup = outbound_lookup[outbound_lookup.within_office_hours == False]
        # Short duration
    outbound_within_office_hours_short_duration_lookup = outbound_within_office_hours_lookup[outbound_within_office_hours_lookup.duration_greater_than_31 == False]
    outbound_outside_office_hours_short_duration_lookup = outbound_outside_office_hours_lookup[outbound_outside_office_hours_lookup.duration_greater_than_31 == False]
        # Long duration   
    outbound_within_office_hours_long_duration_lookup = outbound_within_office_hours_lookup[outbound_within_office_hours_lookup.duration_greater_than_31 == True]
    outbound_outside_office_hours_long_duration_lookup = outbound_outside_office_hours_lookup[outbound_outside_office_hours_lookup.duration_greater_than_31 == True]

    inbound_within_office_hours_short_duration_calls = str(inbound_within_office_hours_short_duration_lookup.number_of_calls.sum())
    inbound_within_office_hours_long_duration_calls = str(inbound_within_office_hours_long_duration_lookup.number_of_calls.sum())
    inbound_outside_office_hours_short_duration_calls = str(inbound_outside_office_hours_short_duration_lookup.number_of_calls.sum())
    inbound_outside_office_hours_long_duration_calls = str(inbound_outside_office_hours_long_duration_lookup.number_of_calls.sum())
    outbound_within_office_hours_short_duration_calls = str(outbound_within_office_hours_short_duration_lookup.number_of_calls.sum())
    outbound_within_office_hours_long_duration_calls = str(outbound_within_office_hours_long_duration_lookup.number_of_calls.sum())
    outbound_outside_office_hours_short_duration_calls = str(outbound_outside_office_hours_short_duration_lookup.number_of_calls.sum())
    outbound_outside_office_hours_long_duration_calls = str(outbound_outside_office_hours_long_duration_lookup.number_of_calls.sum())

    return inbound_within_office_hours_short_duration_calls, inbound_within_office_hours_long_duration_calls, inbound_outside_office_hours_short_duration_calls, inbound_outside_office_hours_long_duration_calls, outbound_within_office_hours_short_duration_calls, outbound_within_office_hours_long_duration_calls, outbound_outside_office_hours_short_duration_calls, outbound_outside_office_hours_long_duration_calls

"""If member member number matches a Gwent coding, reassign to their "real" lca member number."""
def reassign_gwent_member_numbers(member_number):
    if re.fullmatch('60/0017', member_number):
        return 'GWENT-CAERPHILLY'
    if re.fullmatch('60/0006', member_number):
        return 'GWENT-TORFAEN'
    if re.fullmatch('60/0029', member_number):
        return 'GWENT-NEWPORT'
    if re.fullmatch('60/0003', member_number):
        return 'GWENT-MONMOUTHSHIRE'
    return member_number

"""Look up our LogTables to find historical Adviceline groupings."""
def lookup_adviceline_group(lookup_number, member_name, lookup_date, logtables_df):
    pd.to_datetime(lookup_date)
    historical_group_df = logtables_df[(logtables_df.Old_Membership_Number.str.fullmatch(lookup_number, na=False)) & (pd.to_datetime(logtables_df.Start_Date) <= lookup_date) & (pd.to_datetime(logtables_df.End_Date) >= lookup_date) ]
    if len(historical_group_df) == 0: #If member number returns no matches, try matching on name instead.
        historical_group_df = logtables_df[(logtables_df.Old_LCA_Name == member_name) & (pd.to_datetime(logtables_df.Start_Date) <= lookup_date) & (pd.to_datetime(logtables_df.End_Date) >= lookup_date)]
    if len(historical_group_df) >= 1:
        return(historical_group_df.Old_Ops_Group.iloc[0])
    else:
        if lookup_number == '70/0022': #Special case for three rivers CA. It spans two groups in our old phone logic, but we only want to assign it to Hertfordshire here.
            return "Hertfordshire"
        return("Not part of Adviceline")


def email_data_lookup(email_df, member_number, day):
    special_group_names = ["Cardiff & Vale - SAF Remote Service", "Proof of Concept Caerphilly", "Specialist Provider Gateshead"]
    special_member = bool('-' in member_number) # If the member number has a subsidiary dash, it's a special member.
    if special_member:
        # If it's a special member, look it up via its name. 
        special_email_name = special_email_name_lookup(member_number)
        email_lookup = email_df[(email_df.closed_date == day)]
        email_lookup = email_lookup[email_lookup.name.str.contains(special_email_name, na=False, regex=False)]
    else: # Otherwise, look it up normally.
        email_lookup = email_df[(email_df.closed_date == day)]
        #email_lookup = email_lookup[~email_lookup.name.isin(special_group_names)] #Filter out any cases where the name is of a special group... (shouldn't be needed - we're full matching now.)
        email_lookup = email_lookup[email_lookup.member_number.str.fullmatch(member_number, na=False)]
    return str(email_lookup.number_of_emails.sum()), str(email_lookup.responses_under_4_days.sum()), str(email_lookup.responses_under_2_days.sum())

def special_email_name_lookup(member_number):
    if member_number == '60/0052-1':
        return "Cardiff & Vale - SAF Remote Service"
    elif member_number == '60/0017-1':
        return "Proof of Concept Caerphilly"
    elif member_number == '20/0003-1':
        return "Specialist Provider Gateshead"
    else:
        return "Not a group that takes emails."