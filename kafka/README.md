# Smart Transport Prediction - P1

## Setup

docker-compose up -d

## Install Dependency

py -m pip install -r requirements.txt

## Create Topics

py create_topics.py

## Run Producers

py producer_generator.py
py producer_bmkg.py
py producer_events.py

## Run Consumer

py consumer_all.py