import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
import matplotlib.pyplot as plt
import psycopg2




def main():

    global spark, ingest_datetime

    # Initialize Spark session
    spark = SparkSession.builder \
    .config("spark.jars", "D:\\personal\\century_health_assignment\\jars\\postgresql-42.7.3.jar")\
    .appName("patients_data_analysis_app") \
    .getOrCreate()

    ingest_datetime = datetime.utcnow()

    clean()

def connect():

    # connect to source database in our case, we are using files
    patients_df = spark.read.csv(header=True,path='source_data/patients.csv')
    conditions_df = spark.read.csv(header=True,path='source_data/conditions.csv')
    medications_df = spark.read.csv(header=True,path='source_data/medications.csv')
    encounters_df = spark.read.parquet('source_data/encounters.parquet')
    symptoms_df = spark.read.csv(header=True,path='source_data/symptoms.csv')
    patients_gender_df = spark.read.csv(header=True,path='source_data/patients_gender.csv')

    patients_df.printSchema()
    encounters_df.printSchema()
    conditions_df.printSchema()
    medications_df.printSchema()
    symptoms_df.printSchema()

    return patients_df,conditions_df,medications_df,encounters_df,symptoms_df,patients_gender_df


def clean():

    patients_df,conditions_df,medications_df,encounters_df,symptoms_df,patients_gender_df =  connect()

    patients_df.show()

    patients_df = patients_df.select(
    patients_df['PATIENT_ID'].alias('patient_id'),
    patients_df['BIRTHDATE'].alias('birth_date'),
    patients_df['DEATHDATE'].alias('death_date'),
    patients_df['SSN'].alias('social_security_number'),
    patients_df['FIRST'].alias('first_name'),
    patients_df['LAST'].alias('last_name'),
    patients_df['RACE'].alias('race'),
    patients_df['ADDRESS'].alias('address'),
    patients_df['CITY'].alias('city'),
    patients_df['STATE'].alias('state'),
    patients_df['COUNTY'].alias('county'),
    patients_df['ZIP'].alias('zip_code'),
    patients_df['LAT'].alias('latitude'),
    patients_df['LON'].alias('longitude'),
    )

    patients_gender_df = patients_gender_df.withColumnRenamed('id','patient_id') \
    .withColumnRenamed('GENDER','gender')

    patients_df = patients_df.join(patients_gender_df,['patient_id'],'left')

    url = "jdbc:postgresql://localhost:5432/bronze"
    properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
    }

    create_table()

    patients_df.write \
    .jdbc(url=url, table="patients", mode="append", properties=properties)
    return

    conditions_df = conditions_df.select(
    conditions_df['START'].alias('onset_date'),
    conditions_df['STOP'].alias('resolved_date'),
    conditions_df['PATIENT'].alias('patient_id'),
    conditions_df['ENCOUNTER'].alias('encounter_id'),
    conditions_df['CODE'].alias('source_code'),
    conditions_df['DESCRIPTION'].alias('source_description'))

    medications_df = medications_df.select(
    medications_df['START'].alias('medication_start_date'),
    medications_df['STOP'].alias('medication_end_date'),
    medications_df['PATIENT'].alias('patient_id'),
    medications_df['ENCOUNTER'].alias('encounter_id'),
    medications_df['CODE'].alias('source_code'),
    medications_df['DESCRIPTION'].alias('source_description'))

    symptoms_df = symptoms_df.select(
    symptoms_df['PATIENT'].alias('patient_id'),
    symptoms_df['AGE_BEGIN'].alias('age'),
    symptoms_df['PATHOLOGY'].alias('pathology'),
    symptoms_df['NUM_SYMPTOMS'].alias('number_of_symptoms'),
    symptoms_df['SYMPTOMS'].alias('symptoms')) 

    symptoms_df_mod = symptoms_df \
    .withColumn("symptom_array", split(symptoms_df["symptoms"], ";")) \
    .withColumn("symptom_ele", explode("symptom_array")) \
    .withColumn("symptom_name", regexp_extract("symptom_ele", r"([^:]+)", 1)) \
    .withColumn("symptom_value", regexp_extract("symptom_ele", r"(\d+)", 0))

    symptoms_df_mod = symptoms_df_mod.groupBy("patient_id").pivot("symptom_name").agg(first("symptom_value")) \
    .withColumnRenamed('Fatigue','fatigue_level')\
    .withColumnRenamed('Fever','fever_level')\
    .withColumnRenamed('Joint Pain','joint_pain_level')\
    .withColumnRenamed('Rash','rash_level')

    symptoms_df = symptoms_df.join(symptoms_df_mod,['patient_id']).drop('symptoms')

    encounters_df = encounters_df.select(
    encounters_df['START'].alias('encounter_start_date'),
    encounters_df['STOP'].alias('encounter_end_date'),
    encounters_df['PATIENT'].alias('patient_id'),
    encounters_df['id'].alias('encounter_id'),
    encounters_df['CODE'].alias('admit_type_code'),
    encounters_df['DESCRIPTION'].alias('admit_type_description'))

    encounters_df.select('admit_type_description').distinct().show()

    print('distinct patients -----------------')

    patients_count = patients_df.select('patient_id').distinct().count()
    print(patients_count)

    df_grouped = patients_df.groupBy("race", "gender").agg(count("patient_id").alias("patient_count"))

    df_grouped = df_grouped.withColumn("percentage", (col("patient_count") / patients_count) * 100)

    df_grouped.show()

    # Convert the PySpark DataFrame to Pandas DataFrame for plotting
    pandas_df = df_grouped.toPandas()

    pandas_df['label'] = pandas_df['race'] + " - " + pandas_df['gender']

    # Plotting the pie chart
    plt.figure(figsize=(14, 8))
    plt.pie(
    pandas_df['percentage'],
    labels=pandas_df['label'],
    autopct='%1.1f%%',
    startangle=140
)
    plt.title('Percentage of Patients by Racial Category and Gender')
    plt.show()

    # What percentage of patients have all 4 symptom categories ≥ 30?

    # symptoms_df = symptoms_df.

    symptoms_df_p = symptoms_df.filter((col('number_of_symptoms') == lit(4)) & (col('age') >= lit(30)))





def create_table():

    conn = psycopg2.connect(
    host="localhost",
    database="bronze",
    user="airflow",
    password="airflow"
)

    # Create a cursor object
    cur = conn.cursor()

    # Create table SQL statement
    create_table_sql = """
    CREATE TABLE patients (
    patient_id VARCHAR(50) PRIMARY KEY,
    birth_date VARCHAR(20),
    death_date VARCHAR(20),
    social_security_number VARCHAR(50),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    race VARCHAR(20),
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(20),
    county VARCHAR(20),
    zip_code VARCHAR(10),
    latitude VARCHAR(20),
    longitude VARCHAR(20),
    gender VARCHAR(10)
    );
    """

    # Execute the SQL statement
    cur.execute(create_table_sql)

    # Commit changes
    conn.commit()

    # Close the connection
    cur.close()
    conn.close()





if __name__ == '__main__':
    main()
