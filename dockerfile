FROM python:3.5.2

RUN apt-get update -qq && pip3 install Flask && pip3 install psycopg2-binary && sudo apt-get install postgresql-client-10
COPY ./app/ /app

WORKDIR /app