### Youtube Data Harvesting and Warehousing using MongoDB, MYSQL and Streamlit

  The aim of this project is to develop a user-friendly Streamlit application which takes the Youtube
API to extract information from a YouTube channel, stores them in MongoDB as a data lake, migrates 
them to SQL database, and displaying the data in the Streamlit app.

### Tools Used:
  
  *Python
  
  *MongoDB
  
  *Youtube API
  
  *MySql
  
  *Streamlit

### Approach: 

  #Connect to the YouTube API:  We retrieve channel data from youtube with YouTube API. We can use the Google API client library for Python to make requests to the API.

  #Store data in a MongoDB data lake: After collecting channel data, we store them in a MongoDB data lake where it can handle unstructured and semi-structured data easily.

  #Migrate data to a SQL data warehouse: From MongoDB, we can migrate the data to MYSQL database to make them as a structured one. 

  #Query the SQL data warehouse: We use SQL queries to join the tables in the SQL data warehouse and retrieve data for specific channels based on user input. 

  #Display data in the Streamlit app: Finally, we display the retrieved data in the Streamlit app. Streamlit is a great choice for building data visualization and analysis tools quickly and easily. Streamlit creates a simple UI where users can enter a YouTube channel ID, view the channel details, and select channels to migrate them to the data warehouse.

### Skills Take Away:
Python scripting, API integration, Data collection, Data Management using MongoDB and SQL
  


