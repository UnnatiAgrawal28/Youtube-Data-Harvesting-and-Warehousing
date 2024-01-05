
#Import the required set of libraries

import googleapiclient
from googleapiclient.discovery import build
import pandas as pd
import streamlit as st
import pymongo
from urllib.error import HTTPError
from googleapiclient.errors import HttpError
import sqlite3
import sqlalchemy
import json

#--------------------------------------------------------------------------x----------------------------------------------------------------------#
# Comfiguring Streamlit
st.set_page_config(layout='wide')

# Title
st.title(':red[Youtube Data Harvesting and Warehousing]')

#col1, col2 = st.columns(2)
#with col1:
st.header(':blue[Data Collection]')
st.write ('(The data will be collected and stored in :violet[MongoDB] database.)')
channel_id = st.text_input('**Enter channel_id**')
st.write('''Copy the channel_id from Youtube ---> Channel name ---> About ---> Share Channel ---> Copy channel Id  and store it in the MongoDB by clicking below''')
Get_data = st.button('**Store in MongoDB**')

#--------------------------------------------------------------------------x----------------------------------------------------------------------#

#Connect with MongoDB
#Enter Client Server
client=pymongo.MongoClient("mongodb://localhost:27017")

#Enter Database name
db=client["Youtube_dataharvesting"]

#Collection Creation
coll1 = db["channel_stats"]

#--------------------------------------------------------------------------x----------------------------------------------------------------------#

#Youtube Info
api_key= "AIzaSyB6CxuUJOw0LQdbKhGLRAVoKor8P2mbEb0"
api_service= "youtube"
api_version= "v3"
youtube= build(api_service, api_version, developerKey=api_key)

#--------------------------------------------------------------------------x----------------------------------------------------------------------#


#Main Data Coolection
#Channel Info Collection

def channel_stats(channel_id):
    
    channel_information=[]
    
    request = youtube.channels().list(part="snippet,contentDetails,statistics",id= channel_id)
    
    response = request.execute()
    existing_channel_ids = set(coll1.distinct("channel_information.0.channel_id"))

    
    for i in range(len(response['items'])):
        
        data=dict( channel_id = response['items'][i]['id'],
                   channel_name= response['items'][i]['snippet']['title'], 
                   channel_description= response['items'][i]['snippet']['description'], 
                   channel_publish= response['items'][i]['snippet']['publishedAt'],
                   channel_Subscount= response['items'][i]['statistics']['subscriberCount'],
                   channel_Viewcount= response['items'][i]['statistics']['viewCount'],
                   channel_Videocount= response['items'][i]['statistics']['videoCount'],
                   playlist_id= response['items'][i]['contentDetails']['relatedPlaylists']['uploads']
                 )
        if data['channel_id'] in existing_channel_ids:
            st.warning(f"Channel with ID {data['channel_id']} already exists in MongoDB. Skipping.")
        else:
            channel_information.append(data)
        channel_information.append(data)

    
                
    return(channel_information)
    
#-----x------#

# Playlist_id collection
def playlist_id(channel_id): 
    
    playlist_id=[]
    
    response = youtube.channels().list(part="snippet,contentDetails,statistics",id= channel_id).execute()
    
    for i in range(len(response['items'])):
        
        data= response['items'][i]['contentDetails']['relatedPlaylists']['uploads']
        
        playlist_id.append(data)
    
    
    return(playlist_id)
#-----x------#

Playlist_id =playlist_id(channel_id)




#Video_id Collection
def video_id(Playlist_id):
    
    response1 =youtube.playlistItems().list(part="contentDetails", playlistId= ",".join(Playlist_id), maxResults= 50).execute()
    
    video_id_list=[]
    
    
    
    for i in range(len(response1['items'])):
        
        video_id_list.append(response1['items'][i]['contentDetails']['videoId'])
    
    nextpage= response1.get('nextPageToken')
    no_of_pages=True
    
    while no_of_pages:
        if nextpage is None:
            no_of_pages=False
        else:
             response1 =youtube.playlistItems().list(part="contentDetails", playlistId= ",".join(Playlist_id), maxResults= 50,pageToken= nextpage).execute()
            
        for i in range(len(response1['items'])):
            video_id_list.append(response1['items'][i]['contentDetails']['videoId'])
                
        nextpage= response1.get('nextPageToken')

    
    return (video_id_list)

#-----x------#
Video_id=video_id(Playlist_id)



#Video_info Collection


def video_details(Video_id):
    
    video_stats=[]
    

    def time_duration(t):
        a = pd.Timedelta(t)
        b = str(a).split()[-1]
        return b
   
    
    for i in range(0,len(Video_id),50):
        try: 
            response3= youtube.videos().list(part="contentDetails, snippet, statistics", id= Video_id[i:i+50]).execute()
       
        
            for video in response3['items']:
                data1= dict(channel_id= video['snippet']['channelId'],
                        Video_id=video['id'], Video_name= video['snippet']['title'],Tags= video.get("tags"), 
                        Published_at= video['snippet']['publishedAt'],Description= video['snippet']['description'], 
                        Thumbnails= video['snippet']['thumbnails'], Duration= time_duration(video['contentDetails']['duration']),
                        View_Count= video['statistics']['viewCount'], 
                        Like_count= video['statistics']['likeCount'], Fav_count= video['statistics']['favoriteCount'],
                        Comment_count= video['statistics']['commentCount'], Caption_status= video['contentDetails']['caption'] )
            
                video_stats.append(data1)
                
        except HttpError as e:
            if e.resp.status == 403 and 'disabled comments' in str(e):
                st.error(f"Comments are disabled for video: {videoid}")
            else:
                st.write(f"An error occurred while retrieving comments for video: {videoid}")
                st.write(f"Error details: {e}")
    
    return (video_stats)

#-----x------#
Video_stats=video_details(Video_id)

#Comment Info Collection
def comment_details(Video_id):
    
   
    comment_stats=[]

    try:
        for videoid in Video_id:
            
            response4= youtube.commentThreads().list(part="snippet", videoId= videoid, maxResults=10).execute()

            for comment in response4['items']:
                data2= dict(Comment_id= comment['snippet']['topLevelComment']['id'],
                                Video_id=comment['snippet']['topLevelComment']['snippet']['videoId'],
                                Comment_text= comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                                Comment_author= comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                Comment_publishedat= comment['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                comment_stats.append(data2)
    
    except HttpError as e:
        if e.resp.status == 403 and 'disabled comments' in str(e):
            st.error(f"Comments are disabled for video: {videoid}")
        
        else:
            st.write(f"An error occurred while retrieving comments for video: {videoid}")
            st.write(f"Error details: {e}")
    
    return (comment_stats)
Comment_info=comment_details(Video_id)


#--------------------------------------------------------------------------x----------------------------------------------------------------------#


#Data Insertion to MongoDB
def channel_detail(channel_id):

    existing_channel = db["channel_stats"].find_one({"channel_information.channel_id": channel_id})
    if existing_channel:
        st.warning(f"Channel with ID {channel_id} already exists in MongoDB.")
        return "Upload aborted."
    
    channel_details= channel_stats(channel_id)
    Playlist_ids= playlist_id(channel_id)
    Video_id= video_id(Playlist_ids)
    video_stats= video_details(Video_id)
    comment_info= comment_details(Video_id)

    
    coll1 = db["channel_stats"]
    coll1.insert_one({"channel_information":channel_details,
                      "playlist_information": Playlist_ids, 
                      "video_information":video_stats,
                       "comment_information":comment_info
                     })
    
    return "upload completed successfully"

channel_detail(channel_id)
#--------------------------------------------------------------------------x----------------------------------------------------------------------#


#Connect SQL
connection_obj= sqlite3.connect("Youtube_dataharvesting1.db")

#--------------------------------------------------------------------------x----------------------------------------------------------------------#


def data_from_channelinfo(channel_stats):
    client=pymongo.MongoClient("mongodb://localhost:27017")
    db=client["Youtube_dataharvesting"]
    coll1 = db["channel_stats"]
    
    connection_obj= sqlite3.connect("Youtube_dataharvesting1.db")
    
    ch_list = []
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):

        for i in range(len(ch_data["channel_information"])):
            ch_list.append(ch_data["channel_information"][i])
    
        
    df=pd.DataFrame(ch_list)
    df = df.reindex(columns=['channel_id', 'channel_name', 'channel_Subscount', 'channel_Videocount', 'channel_Viewcount',
                            'channel_description'])
    
    df['channel_Subscount'] = pd.to_numeric(df['channel_Subscount'])
    df['channel_Videocount'] = pd.to_numeric(df['channel_Videocount'])
    df['channel_Viewcount'] = pd.to_numeric(df['channel_Viewcount'])

    dtype_dict = {'channel_id': 'VARCHAR', 'channel_name': 'TEXT', 'channel_Subscount': 'INTEGER', 
                  'channel_Videocount': 'INTEGER', 
                  'channel_Viewcount': 'INTEGER', 'channel_description': 'TEXT'}
    
    df.to_sql(name="channels", con=connection_obj, if_exists= 'replace',dtype=dtype_dict )
    

    return "Data in SQL"


def data_from_playlistinfo(channel_stats):
    client=pymongo.MongoClient("mongodb://localhost:27017")
    db=client["Youtube_dataharvesting"]
    coll1 = db["channel_stats"]
    
    connection_obj= sqlite3.connect("Youtube_dataharvesting1.db")
    cursor_obj= connection_obj.cursor() 
  
    connection_obj.commit()
    
    
    ch_list = []
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):

        for i in range(len(ch_data["channel_information"])):
            ch_list.append(ch_data["channel_information"][i])
    
        
    df=pd.DataFrame(ch_list)
    df = df.reindex(columns=['playlist_id', 'channel_id',])
    
    df.to_sql(name="Playlist", con=connection_obj, if_exists= 'replace' )
  

    return "Data in SQL"


def data_from_videoinfo(channel_stats):
    client=pymongo.MongoClient("mongodb://localhost:27017")
    db=client["Youtube_dataharvesting"]
    coll1 = db["channel_stats"]
    
    connection_obj= sqlite3.connect("Youtube_dataharvesting1.db")
    cursor_obj= connection_obj.cursor() 
    
    video_list = []
    
    for video_data in coll1.find({},{"_id":0,"video_information":1}):

        for i in range(len(video_data["video_information"])):
            video_list.append(video_data["video_information"][i])
        
    df=pd.DataFrame(video_list)
    df = df.reindex(columns=['channel_id','Video_id', 'Video_name', 'Description',
                             'Published_at', 'View_Count',
                             'Like_count', 'Fav_count', 'Comment_count', 
                             'Duration','Thumbnails', 'Caption_status'])

    df['Published_at'] = pd.to_datetime(df['Published_at']).dt.date
        
    df['View_Count'] = pd.to_numeric(df['View_Count'])
    df['Like_count'] = pd.to_numeric(df['Like_count'])
    df['Fav_count'] = pd.to_numeric(df['Fav_count'])
    df['Comment_count'] = pd.to_numeric(df['Comment_count'])
    df['Duration'] = pd.to_datetime(df['Duration'], format='%H:%M:%S').dt.time
    
    df_cleaned = df.drop(columns=['Thumbnails'])
    
    dtype_dict= {'channel_id': 'VARCHAR', 'Video_id': 'VARCHAR', 'Video_name': 'TEXT', "Description" : 'TEXT',
                 'Published_at':"DATETIME", 'View_Count': "INTEGER", 'Like_count': 'INTEGER', 
                 'Fav_count': 'INTEGER', 'Comment_count': 'INTEGER', 'Duration':"DATE",'Caption_status':'TEXT'}
    
    df_cleaned[:0].to_sql(
        name="Videos", con=connection_obj, if_exists="replace", index=False, dtype=dtype_dict
    )

    # Check for existing video IDs in the SQL table
    existing_video_ids = pd.read_sql("SELECT Video_id FROM Videos", connection_obj)[
        "Video_id"
    ].tolist()

    # Filter out rows with existing video IDs
    df_to_upload = df_cleaned[~df_cleaned["Video_id"].isin(existing_video_ids)]

    if not df_to_upload.empty:
        # Append the new data to the SQL table
        df_to_upload.to_sql(
            name="Videos", con=connection_obj, if_exists="append", index=False, dtype=dtype_dict
        )
        return "Data in SQL: Upload completed successfully"
    else:
        return "Data in SQL: No new data to upload"

def data_from_commentinfo(channel_stats):
    client=pymongo.MongoClient("mongodb://localhost:27017")
    db=client["Youtube_dataharvesting"]
    coll1 = db["channel_stats"]
    
    connection_obj= sqlite3.connect("Youtube_dataharvesting1.db")
    
    comment_list = []
    for comm_data in coll1.find({},{"_id":0,"comment_information":1}):

        for i in range(len(comm_data["comment_information"])):
            comment_list.append(comm_data["comment_information"][i])
        
    df=pd.DataFrame(comment_list)
    df = df.reindex(columns=['Comment_id', 'Comment_text', 'Comment_author',
                                 'Comment_publishedat', 'Video_id'])
    df['Comment_publishedat'] = pd.to_datetime(df['Comment_publishedat']).dt.date
    
    df.to_sql(name="Comments", con=connection_obj, if_exists= 'replace' )
    
    return "Data in SQL"
    
#--------------------------------------------------------------------------x----------------------------------------------------------------------#

st.header(':blue[MongoDB to SQL ]')
    
# Connect to the MongoDB server
client=pymongo.MongoClient("mongodb://localhost:27017")
db=client["Youtube_dataharvesting"]
coll1 = db["channel_stats"]
connection_obj = sqlite3.connect("Youtube_dataharvesting1.db")


channel_names = []
for ch in coll1.find({},{"_id":0,"channel_information":1}):
    channel_names.append(ch["channel_information"][0]["channel_name"])
ch_name = st.selectbox('**Select Channel name**', options = channel_names, key='channel_names')
st.write('''By clicking below the data will be migrated ftom MongoDB database to :violet[SQLite] database''')
Migrate = st.button('**Migrate to SQL**')
  

result = coll1.find_one({"channel_information.0.channel_name": ch_name})
if result:
    connection_obj = sqlite3.connect("Youtube_dataharvesting1.db")
    data_from_channelinfo(channel_stats)
    data_from_playlistinfo(channel_stats)
    data_from_videoinfo(channel_stats)
    data_from_commentinfo(channel_stats)

    st.success("Data migrated to SQLite successfully.")
       
else:
            
    st.warning(f"No data found for the selected channel name: {document_name}")
#--------------------------------------------------------------------------x----------------------------------------------------------------------#
###Main Questions
question = st.selectbox('Select a query:',
                        ['1. What are the names of all the videos and their corresponding channels?',
                         '2. Which channels have the most number of videos, and how many videos do they have?',
                         '3. What are the top 10 most viewed videos and their respective channels?',
                         '4. How many comments were made on each video, and what are their corresponding video names?',
                         '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                         '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                         '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                         '8. What are the names of all the channels that have published videos in the year 2022?',
                         '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                         '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])

# Connect to SQLite
connection_obj = sqlite3.connect("Youtube_dataharvesting1.db")
cursor = connection_obj.cursor()

# Execute selected query
if question == '1. What are the names of all the videos and their corresponding channels?':
    cursor.execute("""SELECT channels.channel_name AS ChannelName, Videos.Video_name AS VideoTitle 
                      FROM channels JOIN Videos ON channels.channel_id =Videos.channel_id;""")
    
    result_1 = cursor.fetchall()

    # Display the result using st.dataframe
    st.write("Names of All Videos and Their Corresponding Channels:")
    if result_1:
        df_1 = pd.DataFrame(result_1, columns=["Channel Name", "Video Title"])
        st.dataframe(df_1)
    else:
        st.write("No data available.")

elif question == '2. Which channels have the most number of videos, and how many videos do they have?':

    cursor.execute("""
        SELECT channel_id, channel_Name, MAX(channel_Videocount) AS max_video_count
        FROM channels
        GROUP BY channel_id, channel_Name
        ORDER BY max_video_count DESC;
    """)
    result_2 = cursor.fetchall()

    # Display the result using st.dataframe
    st.write("Channels with the Most Number of Videos:")
    if result_2:
        df_2 = pd.DataFrame(result_2, columns=['Channel ID', 'Channel Name', 'Max Video Count'])
        st.dataframe(df_2)
    else:
        st.write("No data available.")

#     cursor.execute("SELECT channel_Name, channel_Videocount FROM channels ORDER BY channel_Videocount DESC;")
#     result_2 = cursor.fetchall()

# # Display the result using st.dataframe
#     st.write("Channels with the Most Number of Videos:")
#     if result_2:
#         df_2 = pd.DataFrame(result_2, columns=['Channel Name', 'Video Count'])
#         st.dataframe(df_2.style.highlight_max(axis=0, color='yellow'))
#     else:
#         st.write("No data available.")
            
            
elif question == '3. What are the top 10 most viewed videos and their respective channels?':
    cursor.execute("""SELECT channels.channel_Name, Videos.Video_name, Videos.View_count
                      FROM channels JOIN Videos ON channels.channel_id = Videos.channel_id
                      ORDER BY Videos.View_count DESC LIMIT 10;""")
    
    result_3 = cursor.fetchall()

# Display the result using st.dataframe
    st.write("Top 10 Most Viewed Videos and Their Respective Channels:")
    if result_3:
        # Convert the result to a DataFrame
        df_3 = pd.DataFrame(result_3, columns=['Channel Name', 'Video Name', 'View Count'])
        st.dataframe(df_3)
    else:
        st.write("No data available.")
    

elif question == '4. How many comments were made on each video, and what are their corresponding video names?':
    # Execute SQL query
    cursor.execute(""" SELECT Videos.Video_name, COUNT(Comments.Comment_id) as Comment_count
                        FROM Videos LEFT JOIN Comments ON Videos.Video_id = Comments.Video_id
                        GROUP BY Videos.Video_name ORDER BY Comment_count DESC; """)
    
    result_4 = cursor.fetchall()
    # Convert the result to a Pandas DataFrame
    columns = ["Video Name", "Comment Count"]
    df_4 = pd.DataFrame(result_4, columns=columns)

    # Display the result using st.dataframe
    st.write("Number of Comments Per Video:")
    if df_4.empty:
        st.write("No data available.")
    else:
        st.dataframe(df_4.style.highlight_max(axis=0, color='yellow'))

elif question == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':

    cursor.execute("""SELECT channels.channel_name AS ChannelName, Videos.Video_name AS VideoTitle, Videos.Like_count 
                       FROM channels JOIN Videos ON channels.channel_id = Videos.channel_id 
                       ORDER BY Videos.Like_count DESC LIMIT 10; """)
    
    result_5 = cursor.fetchall()
    st.write("Video having the Highest Number of Likes:")

    # Display the result using st.dataframe
    if result_5:
        df_5 = pd.DataFrame(result_5, columns=["Channel Name", "Video Title", "Like Count"])
        st.dataframe(df_5)
    else:
        st.write("No data available.")

elif question =='6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':

    cursor.execute(""" SELECT channels.channel_name AS ChannelName, Videos.Video_name AS VideoTitle,  
                        SUM(Videos.Like_count) AS TotalLikes
                        FROM channels  JOIN Videos ON channels.channel_id = Videos.channel_id 
                        GROUP BY ChannelName, VideoTitle;""")

    # Fetch the results
    result_6 = cursor.fetchall()
    st.write("Number of Likes per Video:")

    # Display the result using st.dataframe
    if result_6:
        df_6= pd.DataFrame(result_6, columns=["Channel Name", "Video Title", "Total Likes"])
        st.dataframe(df_6)
    else:
        st.write("No data available.")

elif question =='7. What is the total number of views for each channel, and what are their corresponding channel names?':
    # Execute SQL query
    cursor.execute("""SELECT channels.channel_name AS ChannelName, SUM(Videos.View_count) AS TotalViews 
                    FROM channels JOIN Videos ON channels.channel_id = Videos.channel_id
                    GROUP BY ChannelName;""")
    
    # Fetch the results
    result_7 = cursor.fetchall()
    
    # Display the result using st.dataframe
    st.write("Total Number of Views per Channel:")
    if result_7:
        # Convert the result to a DataFrame
        df_7 = pd.DataFrame(result_7, columns=["Channel Name", "Total Views"])
        st.dataframe(df_7)
    else:
        st.write("No data available.")


elif question == '8. What are the names of all the channels that have published videos in the year 2022?':
    
    cursor.execute(""" SELECT DISTINCT channels.channel_name AS ChannelName, Videos.Published_at AS PublishedDate
                       FROM channels JOIN Videos ON channels.channel_id = Videos.channel_id
                       WHERE strftime('%Y', Videos.Published_at) = '2022';""")
    
    result_8 = cursor.fetchall()

    # Display the result using Streamlit
    st.write("Channels with videos published in 2022:")
    if result_8:
        df_8 = pd.DataFrame(result_8, columns=["Channel Name", "Published Date"])
        st.dataframe(df_8)
    else:
        st.write("No data available.")

elif question =='9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    cursor.execute("""SELECT channels.channel_name AS ChannelName, 
                            AVG(strftime('%s', Videos.Duration)) AS AverageDurationInSeconds
                    FROM channels 
                    JOIN Videos ON channels.channel_id = Videos.channel_id
                    GROUP BY ChannelName;""")
    
    # Fetch the results
    result_9 = cursor.fetchall()
    
    # Display the result using st.write
    st.write("Average Duration of Videos in Each Channel:")
    if result_9:
        df_9 = pd.DataFrame(result_9, columns=["Channel Name", "Average Duration (Seconds)"])
        st.dataframe(df_9)
    else:
        st.write("No data available.")

elif question =='10. Which videos have the highest number of comments, and what are their corresponding channel names?':
    # Execute SQL query
    cursor.execute("""SELECT channels.channel_name AS ChannelName, 
                            Videos.Video_name AS VideoTitle, 
                            Videos.Comment_count AS CommentCount
                    FROM channels 
                    JOIN Videos ON channels.channel_id = Videos.channel_id
                    ORDER BY CommentCount DESC
                    LIMIT 10;""")
    
    # Fetch the results
    result_10 = cursor.fetchall()
    
    # Display the result using st.write
    st.write("Videos with the Highest Number of Comments:")
    if result_10:
        df_10 = pd.DataFrame(result_10, columns=["Channel Name", "Video Title", "Comment Count"])
        st.dataframe(df_10)
    else:
        st.write("No data available.")
    


#  Close SQLite connection
cursor.close()
connection_obj.close()
