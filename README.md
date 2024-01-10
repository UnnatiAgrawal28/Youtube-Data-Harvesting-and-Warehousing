# Youtube-Data-Harvesting-and-Warehousing
Outline:
The data is collected from Youtube API using python and then stored in MongoDB data base. As per the queries required the data is accessed using SQLite, Pandas, and other libraries of Python.

Detailed Description:
The provided code is a Python script that uses the Streamlit library to create a web application for fetching data from the YouTube API,storing it in a MongoDB database, and uploading it to a SQL database for further analysis. Here's a brief explanation of the code:

The script imports necessary libraries and modules, including Streamlit, Google API client, pymongo, datetime, time, pandas, HTTP Error sqlite3 etc.

It sets the Streamlit page configuration and displays a title for the web application.

The code defines several functions for fetching data from the YouTube API, including channel statistics, playlist id, video IDs, video details and comment details. These functions use the provided YouTube API key to make API requests and retrieve the desired data.

The code defines a function to fetch channel names from the MongoDB database.

The script creates a connection to the MongoDB database and defines collections for channel data, video data, and comment data.

The code checks for user input (channel ID) and a button click to fetch channel details and upload them to the MongoDB database.It calls the previously defined functions to fetch channel details, playlist data, video IDs, video details, and comment details.The fetched data is then inserted into the corresponding collections in the MongoDB database.

After fetching and uploading the data to MongoDB, the script provides an option for the user to select channels for uploading the data to a SQL database. It displays a multiselect input for channel selection and a button to upload the data to SQLite3.

When the button to upload data to SQLite3 is clicked, the script fetches channel details, video details, and comment details from the MongoDB collections. It establishes a connection to the SQL database and inserts the data into corresponding tables using SQL queries. It uses pandas for the data insertion process.

Finally, the script provides a selection box for the user to choose from several predefined questions. Based on the selected question, the script executes
corresponding SQL queries on the SQL database and retrieves the results. The results are displayed in a table format using pandas dataframe.

Overall, the code combines the functionalities of fetching data from the YouTube API, storing it in MongoDB, uploading it to SQLite3, and displaying the results in a web application using Streamlit.
