from confluent_kafka import Consumer
import os

import numpy as np
import pandas as pd
import pickle
import string
import re
import json
from nltk.corpus import stopwords
import time

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

import pymongo
import psycopg2

import logging

# Configure logging
logging.basicConfig(filename='app.log', level=logging.INFO)

# credentials to connect to database
DB_NAME = "api_logs_db"
DB_USER = "postgres"
DB_PASS = "pravin24"
DB_HOST = "127.0.0.1"
DB_PORT = "5432"
 
# initiate connection to database
pqsql_connection = psycopg2.connect(database=DB_NAME,
                            user=DB_USER,
                            password=DB_PASS,
                            host=DB_HOST,
                            port=DB_PORT)

# create cursor to execute queries
pgsql_cursor = pqsql_connection.cursor()

# function to put the data in table
def update_pgsql_table(log_data):
    # print(type(log_data))
    # print(log_data)
    try:
        query = """
            INSERT INTO public.api_logs_table (text, sentiment, timestamp)
            VALUES (%s, %s, %s)
        """
        
        # Execute the query with the log data values
        pgsql_cursor.execute(query, (log_data['text'], log_data['sentiment'], log_data['timestamp']))
    
    except Exception as e:
        print(e)
        return False
    else:
        pqsql_connection.commit()
        return True
    
# function to remove username from text
def username_remover(input_txt, username):
    """removes the username handle from the data"""
    
    r = re.findall(username, input_txt)
    for i in r:
        input_txt = re.sub(i, '', input_txt)
        
    return input_txt

# function to remove punctuations and stopwords
def cleaner(comment):
    
    """function to perform cleaning of the text data. For any comment
    input the function cleans the punctuations and stopwords."""
    
    punctuation_removed = [char for char in comment if char not in string.punctuation]
    punctuation_removed_join = ''.join(punctuation_removed)
    stops_removed = [word for word in punctuation_removed_join.split() if word.lower() not in stopwords.words('english')]
    
    return stops_removed

# load count vectorizer from pickle file
vectorizer = pickle.load(open("vectorizer.pkl", 'rb'))

# load NB classifier from pickle file
nb_model = pickle.load(open("nb_model.pkl", 'rb'))

# function to predict the sentiment given text as input
def predict_sentiment(text):

    username_removed_text = username_remover(text, "@[\w]*")
    cleaned_text = cleaner(username_removed_text)

    vectorized_text = vectorizer.transform([" ".join(cleaned_text)])
    prediction = nb_model.predict(vectorized_text)[0]
    # prediction 0 is positive and prediction 1 is negative
    if prediction == 0:
        sentiment = "Positive"
    else:
        sentiment = "Negative"

    log_data = {
        "text"      : text,
        "sentiment" : sentiment,
        "timestamp" : time.time(),
        }
    
    pgsql_updated = update_pgsql_table(log_data)  
    if pgsql_updated:
        pgsql_updated_status = "Success"
    else:
        pgsql_updated_status = "Failed"
    
    log_data["pgsql_updated_status"] = pgsql_updated_status
    print(log_data)
    logging.info(json.dumps(log_data))

# Kafka configuration
BROKER = 'localhost:9092'
TOPIC = 'csv-topic'
GROUP = 'csv-consumer-group'
OUTPUT_FILE = 'received_data.txt'

# Consumer configuration
consumer_conf = {
    'bootstrap.servers': BROKER,
    'group.id': GROUP,
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_conf)
consumer.subscribe([TOPIC])

# Ensure the output file exists
if not os.path.exists(OUTPUT_FILE):
    open(OUTPUT_FILE, 'w').close()

print("Waiting for messages...")

try:
    while True:
        msg = consumer.poll(1.0)  # Poll messages every second
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        
        # Decode the received message
        received_text = msg.value().decode('utf-8')
        

        # Send field from received data to predict sentiment and store it into logs
        predict_sentiment(received_text)
        
        print("Received:  ", received_text)

        # Append the received message to the text file
        # with open(OUTPUT_FILE, 'a') as f:
        #     f.write(received_data + '\n')

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:
    consumer.close()
