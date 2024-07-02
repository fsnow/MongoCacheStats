import certifi
import sys
from pymongo import MongoClient
import time
import streamlit as st
import plotly.graph_objects as go

# Check for command-line argument
if len(sys.argv) < 2:
    print("Please provide a MongoDB connection string as a command-line argument.")
    sys.exit(1)

connection_string = sys.argv[1]

# Streamlit setup
st.set_page_config(page_title="MongoDB Cache Usage", layout="wide")
st.title("MongoDB Cache Usage")

# MongoDB connection
try:
    client = MongoClient(connection_string, tlsCAFile=certifi.where())
    db = client.admin
    # Test the connection
    db.command("ping")
except Exception as e:
    st.error(f"Failed to connect to MongoDB: {e}")
    st.stop()

# Radio button for choosing the denominator
denominator_choice = st.radio(
    "Choose the denominator for percentage calculation:",
    ("Sum of Used Cache", "Total WiredTiger Cache")
)

# Function to get collection stats
def get_collection_stats():
    dbInfos = db.command({"listDatabases": 1, "nameOnly": True})
    collection_data = []

    for dbInfo in dbInfos["databases"]:
        dbName = dbInfo["name"]
        current_db = client[dbName]
        collections = current_db.list_collections()

        for collection in collections:
            if collection["type"] == "view":
                continue

            collectionName = collection["name"]
            if collectionName.startswith("system."):
                continue

            collStats = current_db.command("collstats", collectionName)
            
            if "errmsg" in collStats and collStats["errmsg"] == "Collection stats not supported on views":
                continue

            inCache = int(collStats["wiredTiger"]["cache"]["bytes currently in the cache"])
            ns = f"{dbName}.{collectionName}"
            collection_data.append({"name": ns, "inCache": inCache})

            # Add index stats
            for indexName, indexStats in collStats.get("indexDetails", {}).items():
                indexInCache = int(indexStats["cache"]["bytes currently in the cache"])
                indexNs = f"{ns} (index: {indexName})"
                collection_data.append({"name": indexNs, "inCache": indexInCache})

    return collection_data

# Function to create pie chart
def create_pie_chart(data, total_cache_size):
    labels = [item["name"] for item in data]
    values = [item["inCache"] for item in data]
    
    sum_used_cache = sum(values)
    
    if denominator_choice == "Total WiredTiger Cache":
        unused_cache = total_cache_size - sum_used_cache
        if unused_cache > 0:
            labels.append("Unused Cache")
            values.append(unused_cache)
    
    fig = go.Figure(data=[go.Pie(labels=labels, values=values, hole=.3)])
    fig.update_layout(height=800)
    return fig

# Main loop
while True:
    collection_data = get_collection_stats()
    
    # Get total cache size
    server_status = db.command("serverStatus")
    total_cache_size = server_status["wiredTiger"]["cache"]["maximum bytes configured"]
    
    # Create and display pie chart
    fig = create_pie_chart(collection_data, total_cache_size)
    st.plotly_chart(fig, use_container_width=True)
    
    # Display total cache usage information
    sum_used_cache = sum(item["inCache"] for item in collection_data)
    st.info(f"Total Cache Size: {total_cache_size:,} bytes")
    st.info(f"Total Used Cache: {sum_used_cache:,} bytes")
    st.info(f"Cache Usage: {sum_used_cache/total_cache_size:.2%}")
    
    time.sleep(60)
    st.rerun()