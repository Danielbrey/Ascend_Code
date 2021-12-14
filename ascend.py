#   Goal: Access this week's electricity usage for a given building (starting on the most recent Sunday).

#   This will be done by finding the day of the week, and then for each previous day this week, accessing
#   its data, and then merging them together.


import requests
from datetime import datetime, timedelta
import pandas as pd
import io


#   get_url takes in the current day, month, and year and returns a url that contains the data for that specified day.
def get_url(curr_day, curr_month, curr_year):
    #   we need the day and week to be 2 digit strings, so 12-4-2021 needs to be written as 12-04-2021, 
    #   so we add a 0 in front when necessary.
    if(int(curr_day) < 10):
        curr_day = '0' + str(curr_day)
    if(int(curr_month) < 10):
        curr_month = '0' + str(curr_month)    
    #   returns the url that can access this specified date's electricity usage
    return "https://observatory.middlebury.edu/campus/energy/archive/{}{}{}-all.csv".format(curr_year, curr_month, curr_day)




#   Previous Problem: Our data contained too many data points, and the graph of the variable had no smooth transitions,
#   so we want to take only the data from each 10th minute.
 
#   string_filter takes in our dateframe, and manipulates the datetime column so we can filter the data to only that column
def string_filter(df):
    #   the 0th column is the datetime value, so we should only look at it (values look like 2021-12-14T11-06-00)
    datetimes_as_strings = df.iloc[:,0]
    #   replaces the 'T' in the datetime with '-' so we can split using the '-' character
    datetimes_replace = datetimes_as_strings.str.replace('T', '-')
    datetimes_split = datetimes_replace.str.split('-')
    #   the minute is the 4th value in the list [year, month, day, hour, minute, second]
    datetimes_minute = datetimes_split.apply(pd.Series)[4]
    #   filter to only include the rows where the minute is divisible by 10
    minutes_filter = datetimes_minute.astype('int')%10 == 0
    return df[minutes_filter]



#   load_day_data loads the data given a certain day, month, and year (and potentially a building),
#   and returns the dataframe after it is filtered by string_filter (function above)
def load_day_data(curr_day, curr_month = "", curr_year = ""):
    #   using get_url from above
    url = get_url(curr_day, curr_month, curr_year)
    #   using the url, we can access the csv online
    data = requests.get(url).content
    df = pd.read_csv(io.StringIO(data.decode('utf-8')), skiprows=1)
    return string_filter(df)


#   OVERALL FUNCTION: get_week_data potentially takes in a building on campus, and returns a dataframe with all data from this current week
#   (since the most previous Sunday)
#   Note: If a building is not specified, it will end up getting the data for the whole campus' data
def get_week_data(location = None):
    #   We first find the exact date, and also the day of the week (in terms of integers, where Sunday  = 0)
    datetime_rn = datetime.now()
    weekday = datetime_rn.strftime("%w")  
    #   Using the datetime found, we use load_day_data (function above) to acquire today's dataframe
    df = load_day_data(datetime_rn.day, datetime_rn.month, datetime_rn.year)
    df.columns = ['datetime', 'location', 'power']
    #Depending on if a location/building is passed in, we will either filter to only look at that building's data or the whole campus' data
    if(location):
        df = df[df['location'] == location]
    else:
        df = df[df['location'] == 'campus']
    #   For each past day of the week, we want to access its data
    for i in range(int(weekday)):
        #   We access the data backwards so it can be in order
        past_day_date = datetime_rn - timedelta(days = i+1)
        past_day = load_day_data(past_day_date.day, past_day_date.month, past_day_date.year)
        past_day.columns = ['datetime', 'location', 'power']
        #Depending on if a location/building is passed in, we will either filter to only look at that building's data or the whole campus' data
        if(location):
            past_day = past_day[past_day['location'] == location]
        else:
            past_day = past_day[past_day['location'] == 'campus']
        #merge the past day's data with the dataframe we have so far (the past day goes first so it is in order)
        df = pd.merge(past_day, df, how = "outer", on = ['datetime', 'location', 'power'])
    return(df)