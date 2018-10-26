# How to run
Please clone project bcredi-analytics in git hub using git@github.com:bcredi/bcredi-analytics.git
To RUN the container, please run into project Folder and execute below:
- docker-compose up -d

To create a postgres db, we need to have all extractions from GA and Simulator. Fot that, please get all respective files from CARD in pipefy https://app.pipefy.com/pipes/478462#cards/11046682
Please include all files in app folder in project.

After that, you'll be able to run extraction.py and transformation.py respectively.
In extraction.py, you need to inform password after running extraction.py
Password you'll get in docker-compose file configurations in bcredi-analytics project.

In order to get all new tables from Postgres container, just execute below command in prompt:
docker exec -tiu postgres Docker_name psql
If you don't know the Docker_name, please exec "Docker ps" to get it.

The final table is named as simulator_analytics