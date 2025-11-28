import streamlit as st
from pymongo import MongoClient
import pandas as pd

client = MongoClient("mongodb://mongodb:27017")
db = client["streaming_db"]
collection = db["messages"]

st.title("Test Dashboard MongoDB")

data = list(collection.find({}, {"_id": 0}))
df = pd.DataFrame(data)

st.write(df)
