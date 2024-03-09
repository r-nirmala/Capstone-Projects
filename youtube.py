import googleapiclient.discovery
import pandas as pd
import pymongo 
from pymongo import MongoClient
import mysql.connector
import streamlit as st
from streamlit_option_menu import option_menu
from PIL import Image
from datetime import datetime
import re

#Building connection with Youtube API
api_service_name = "youtube"
api_version = "v3"
api_key = "AIzaSyD-dVLGQm6aqsEqaru2Nw4Ic_YoKcFK5Ys"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey = api_key)


#Setting up a connection with MongoDB Atlas and creating a database
con = MongoClient("mongodb+srv://nirmala:T8fW1K5yn16Nh3px@cluster0.fo6rjrx.mongodb.net/------>link")
db = con['Youtube_Data']
col1 = db['channel_details']


#Setting up a connection with MYSQL Database
connection = mysql.connector.connect(host="localhost",user="root",password="12345",database="Youtube_data")
mycursor = connection.cursor()


#Function to get channel info
def get_channel_info(channel_id):
    request = youtube.channels().list(
        part = "snippet,contentDetails,statistics",
        id = channel_id)
    response = request.execute()
    for i in response['items']:
        data = dict(channel_name = i['snippet']['title'],
                    channel_iD = i['id'],
                    subscription_count = i['statistics']['subscriberCount'],
                    channel_views = i['statistics']['viewCount'],
                    total_videos = i['statistics']['videoCount'],
                    channel_description = i['snippet']['description'],
                    playlist_id = i['contentDetails']['relatedPlaylists']['uploads'])
    return data

#function to get video ids
def get_channel_videoid(channel_id):
    video_ids = []
    request = youtube.channels().list(id=channel_id,
                                        part="contentDetails")
    response = request.execute()
    Playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    while True:
        res = youtube.playlistItems().list(part ='snippet',playlistId = Playlist_id,
                                       maxResults = 50,pageToken = next_page_token).execute()
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        
        if next_page_token is None:
            break

    return video_ids


#function to get video info
def get_video_info(Video_Ids):
    videos_data = []
    for vid_id in Video_Ids:
        request = youtube.videos().list(part = "snippet,contentDetails,statistics",id = vid_id)
        response = request.execute()
        for item in response['items']:
            data = dict(channel_name = item['snippet']['channelTitle'],
                        channel_iD = item['snippet']['channelId'],
                        video_id = item['id'],
                        video_name = item['snippet']['title'],
                        Video_Description = item['snippet']['description'],
                        Tags =','.join(item['snippet'].get('tags',['NA'])),
                        PublishedAt = date_time(item['snippet']['publishedAt']),
                        View_Count = item['statistics'].get('viewCount'),
                        Like_Count = item['statistics'].get('likeCount'),
                        Dislike_Count = item['statistics'].get('dislikeCount'),
                        Favorite_Count = item['statistics'].get('favoriteCount'),
                        Comment_Count = item['statistics'].get('commentCount'),
                        Duration = iso_to_hh_mm_ss(item['contentDetails']['duration']),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        caption_status = item['contentDetails']['caption'],
                    )
        videos_data.append(data)
    return videos_data

#function to get datetime format
def date_time(timestamp):
    date_time_obj = datetime.strptime(timestamp,'%Y-%m-%dT%H:%M:%SZ')
    formatted_time = date_time_obj.strftime("%Y-%m-%d %H:%M:%S")
    return formatted_time

# Function to convert ISO duration to hh:mm:ss format
def iso_to_hh_mm_ss(iso_duration):
    # Regular expression pattern to extract hours, minutes, and seconds
    pattern = r"PT(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?"
    match = re.match(pattern, iso_duration)

    if match:
        hours = int(match.group("hours")) if match.group("hours") else 0
        minutes = int(match.group("minutes")) if match.group("minutes") else 0
        seconds = int(match.group("seconds")) if match.group("seconds") else 0

        # Format as hh:mm:ss
        hh_mm_ss_format = f"{hours:02}:{minutes:02}:{seconds:02}"
        return hh_mm_ss_format
    else:
        return "Invalid duration format"



#function to get comment info
def get_comment_info(Video_Ids):
    comments_data = []
    try:
        for vid_id in Video_Ids:
            request = youtube.commentThreads().list(part = "snippet",videoId = vid_id, maxResults = 10)
            response = request.execute()
            for item in response['items']:
                data = dict(Video_Id = item['snippet']['videoId'],
                        Comment_Id = item['snippet']['topLevelComment']['id'],
                        Comment_Text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_PublishedAt = item['snippet']['topLevelComment']['snippet']['publishedAt']
                       )
                comments_data.append(data)
    except:
        pass
    return comments_data
    

#function to insert data into mongoDB
def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    vid_ids = get_channel_videoid(channel_id)
    vid_details = get_video_info(vid_ids)
    comm_details = get_comment_info(vid_ids)

    col1.insert_one({'channel_information':ch_details,'video_information':vid_details,
                     'comment_information':comm_details})




#setting streamlit option menus
with st.sidebar:
    choice = option_menu(None, ["Home","Data collection and uploading to MongoDB","SQL Data Warehousing","Analysing channel data with queries"],
                         icons=["house-door-fill"],
                        styles={"nav-link": {"font-size": "20px", "text-align": "centre", "margin": "0px", 
                                                "--hover-color": "grey"},
                                   "icon": {"font-size": "20px"},
                                   "container" : {"max-width": "4000px"},
                                   "nav-link-selected": {"background-color": "orange"}})


#setting streamlit main pages
if choice == "Home":
    img = Image.open(r"C:\Users\rnirm\OneDrive\Desktop\Youtube.jpg")
    size = (97,40)
    img1 =img.resize(size)
    st.image(img1)
    st.header("Youtube Data Harvesting and Warehousing")
    st.markdown('##')
    st.markdown('##')
    st.markdown('##')
    col1,col2,col3= st.columns(3, gap= 'large')
    col1.subheader(" :violet[Overview]", divider='rainbow')
    col1.write("##### Retrieving youtube channel data from youtube API,storing them in MongoDB as a data lake, migrating datas into MySQL database, querying and displaying the data in streamlit app ") 
    col2.subheader(" :green[Skills Take Away]", divider='rainbow')
    col2.caption('### :blue[Python scripting]')
    col2.caption('### :blue[API integration]')
    col2.caption('### :blue[Data collection]')
    col2.caption('### :blue[Data Management using MongoDB and SQL]')
    col3.subheader(" :red[Technologies used]", divider='rainbow')
    col3.caption('### Python')
    col3.caption('### MongoDB')
    col3.caption('### Youtube Data API')
    col3.caption('### MySql')
    col3.caption('### Streamlit')

if choice == "Data collection and uploading to MongoDB":
    st.subheader(':red[Load Channel data to MongoDB]')
    st.markdown("##    ")
    st.write("##### :green[Enter the YouTube Channel ID]")
    ch_id = st.text_input("### Get Channel ID from channel Page").split(',')
    st.markdown("## ")
    if st.button("### :orange[Upload to MongoDB]"):
        with st.spinner('Please Wait for it...'): 
            db = con['Youtube_Data']
            col1 = db['channel_details']
            chan_ids = []
            for i in col1.find({},{'_id':0}):
                chan_ids.append(i['channel_information']['channel_iD'])
            if ch_id in chan_ids:
                st.success('### :violet[Channel details already uploaded]')
            else:
                insert = channel_details(ch_id)
                st.success("##### :rainbow[Channel Data Successfully stored in MongoDB]") 

#function to get channel names from mongoDB
def channel_names():
    ch_names = []
    for i in col1.find({},{'_id':0}):
        ch_names.append(i['channel_information']['channel_name'])
    return ch_names

# function for Channel Table
def channel_table():
  connection = mysql.connector.connect(host="localhost",user="root",password="12345",database="Youtube_data")
  mycursor = connection.cursor()


  query = '''create table if not exists channels(channel_name varchar(100),
                                                channel_iD varchar(100) primary key,
                                                subscription_count bigint,
                                                channel_views bigint,
                                                total_videos int,
                                                channel_description text,
                                                playlist_id varchar(100))'''
  mycursor.execute(query)
  connection.commit()


  d = col1.find_one({'channel_information.channel_name':user_input},{'_id':0,'channel_information':1})
 


  query = '''insert into channels(channel_name, channel_iD,subscription_count,
            channel_views, total_videos, channel_description,playlist_id)
                          
          values(%s,%s,%s,%s,%s,%s,%s)'''

  values = tuple(d['channel_information'].values())

  mycursor.execute(query,values)
  connection.commit()


#function for Video Table
def video_table():
    connection = mysql.connector.connect(host="localhost",user="root",password="12345",database="Youtube_data")
    mycursor = connection.cursor()


    query = '''create table if not exists videos(channel_name varchar(100),
                                                channel_iD varchar(100),
                                                video_id varchar(50) primary key,
                                                video_name varchar(150),
                                                Video_Description text,
                                                Tags text,
                                                PublishedAt DATETIME,
                                                View_Count bigint,
                                                Like_Count bigint ,
                                                Dislike_Count bigint,
                                                Favorite_Count int,
                                                Comment_Count int,
                                                Duration TIME,
                                                Thumbnail varchar(200) ,
                                                caption_status varchar(100))'''

    mycursor.execute(query)
    connection.commit()

    d = col1.find_one({'channel_information.channel_name':user_input},{'_id':0,'video_information':1})

    query = '''insert into videos(channel_name, channel_iD, video_id, video_name, Video_Description, 
                            Tags, PublishedAt, View_Count, Like_Count, Dislike_Count, Favorite_Count,
                            Comment_Count, Duration, Thumbnail, caption_status)
                                            
                                            
            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

    for i in d['video_information']:
        mycursor.execute(query,tuple(i.values()))
        connection.commit()


#function for Comments Table
def comments_table():
        connection = mysql.connector.connect(host="localhost",user="root",password="12345",database="Youtube_data")
        mycursor = connection.cursor()


        query = '''create table if not exists comments(Video_Id varchar(100),
                                Comment_Id varchar(100) primary key,
                                Comment_Text text,
                                Comment_Author varchar(100) ,
                                Comment_PublishedAt varchar(100))'''
        mycursor.execute(query)
        connection.commit()


        d = col1.find_one({'channel_information.channel_name':user_input},{'_id':0,'comment_information':1})
       

        query = '''insert into comments(Video_Id, Comment_Id, Comment_Text, Comment_Author,Comment_PublishedAt)
                values(%s,%s,%s,%s,%s)'''

        for i in d['comment_information']:      

                mycursor.execute(query,tuple(i.values()))
                connection.commit()
                
#Function to call all the tables
def tables():
    channel_table()
    video_table()
    comments_table()   


if choice == "SQL Data Warehousing":
    st.subheader(' :green[Data Migration from MongoDB to MySQL]')
    st.markdown("##   ")    
    ch_names = channel_names()
    user_input = st.selectbox('###### :orange[Select a channel to migrate]',options = ch_names)
    import_to_sql = st.button("### :red[Migrate to SQL]")
    st.write("Click the button to migrate data")
    if import_to_sql: 
        with st.spinner('Please Wait for it...'):
            try:
                tables()    
                st.success('###### :rainbow[Migration successfull]')
            except:
                st.error('######  Channel details are already migrated')
    st.markdown("##   ")  
    view_tables = st.radio("###### :orange[Select the tables to view]",
                ["***Channels***", "***Videos***", "***Comments***"])
    if view_tables =="***Channels***":
        d = col1.find_one({'channel_information.channel_name':user_input},{'_id':0,'channel_information':1})
        df1 = pd.DataFrame(d)
        st.write(df1)
    elif view_tables == "***Videos***": 
        d = col1.find_one({'channel_information.channel_name':user_input},{'_id':0,'video_information':1})
        vid_tab = []
        for i in d['video_information']:
            vid_tab.append(i)
        df2 = pd.DataFrame(vid_tab)
        st.write(df2)
    elif view_tables == "***Comments***":
        d = col1.find_one({'channel_information.channel_name':user_input},{'_id':0,'comment_information':1})
        com_tab = []
        for i in d['comment_information']:
            com_tab.append(i)
        df3 = pd.DataFrame(com_tab)
        st.write(df3)


if choice == "Analysing channel data with queries":
    st.subheader(' :blue[Queries to get Insights]')
    st.markdown("##    ")
    Qn1 = '1.What are the names of all the videos and their corresponding channels?'
    Qn2 = '2.Which channel have the most number of videos, and how many videos do they have?'
    Qn3 = '3.What are the top 10 most viewed videos and their respective channels?'
    Qn4 = '4.How many comments were made on each video, and what are their corresponding video names?'
    Qn5 = '5.Which videos have the highest number of likes, and what are their corresponding channel names?'
    Qn6 = '6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?'
    Qn7 = '7.What is the total number of views for each channel, and what are their corresponding channel names?'
    Qn8 = '8.What are the names of all the channels that have published videos in the year 2022?'
    Qn9 = '9.What is the average duration of all videos in each channel, and what are their corresponding channel names?'
    Qn10 = '10.Which videos have the highest number of comments, and what are their corresponding channel names?'
    ques = st.selectbox('Select your queries',(Qn1,Qn2,Qn3,Qn4,Qn5,Qn6,Qn7,Qn8,Qn9,Qn10))
    click = st.button('##  :red[SQL]')
    if click:
        connection = mysql.connector.connect(host="localhost",user="root",password="12345",database="Youtube_data")
        mycursor = connection.cursor()
        if ques == Qn1:
            mycursor.execute('''select video_name as Video_Name, channel_name as Channel_Name from 
                             videos order by channel_name''')
            df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
            st.write(df)
        if ques == Qn2:
            mycursor.execute('''select channel_name AS Channel_Name,total_videos AS Total_Videos from channels 
                             ORDER BY total_videos DESC''')
            df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
            st.write(df)    
        if ques == Qn3:
            mycursor.execute('''SELECT channel_name AS Channel_Name, video_name AS video_names, 
                             View_Count AS Views FROM videos ORDER BY View_Count DESC
                            LIMIT 10''')
            df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
            st.write(df)    
        if ques == Qn4:
            mycursor.execute('''select channel_name as Channel_Name, video_name as Video_Names, 
                            Comment_Count as No_of_comments from videos order by Comment_Count DESC''')
            df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
            st.write(df) 
        if ques == Qn5:
            mycursor.execute('''select channel_name as Channel_Name, video_name as Video_Names, 
                            Like_Count as Video_Likes from videos order by Like_count DESC limit 10''')
            df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
            st.write(df) 
        if ques == Qn6:
            mycursor.execute('''select channel_name as Channel_Name, video_name as Video_Names, 
                            Like_Count as Video_Likes from videos order by Like_count DESC''')
            df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
            st.write('###### :orange[In early November 2021,YouTube announced its decision to remove the public-facing “Dislike” count from all videos on the platform]')
            st.write(df) 
        if ques == Qn7:
            mycursor.execute('''select channel_name as Channel_Name, channel_views as Total_views from channels order by channel_views DESC''')
            df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
            st.write(df) 
        if ques == Qn8:
            mycursor.execute('''select channel_name as Channel_Name,PublishedAt as Published_At from videos where PublishedAt LIKE '2022%' order by channel_name''')
            df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
            st.write(df) 
        if ques == Qn9:
            mycursor.execute('''select channel_name as Channel_Name, SEC_TO_TIME(AVG(TIME_TO_SEC(Duration)))  as Avg_video_duration from videos group by channel_name''')
            df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
            st.write(df) 
        if ques == Qn10:
            mycursor.execute('''select channel_name as Channel_Name,video_name as Video_Names, 
                            Comment_Count as comment_count from videos order by Comment_Count DESC limit 10''')
            df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
            st.write(df) 



