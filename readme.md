we just have to install the setups mentioned in requiremnts.txt have pyspark, docker installed locally.

running:
docker compose up airflow-init 
docker-compose up -d

should run the whole app, along with pie chart and results.

This would take care of creating DB and tables automatically for the first run.

The main file is app.py which is in the root folder, it has all the code required to run as spark job locally.

If we want to view the data ingested in DB, we can docker inspect the postgres container id and get the IP address.
open localhost:5050 and, input the login creds: PGADMIN_DEFAULT_EMAIL and PGADMIN_DEFAULT_PASSWORD. Use the IP address as host, user and password can be 'airflow'

Thanks.

