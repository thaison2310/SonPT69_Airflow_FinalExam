from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import os
import json
import psycopg2
from sql_queries import SqlQueries
import re


default_args = {
    'owner': 'genz-legend',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG(
    'tune_stream_dag',
    default_args=default_args,
    description='ETL pipeline for TuneStream using local files + PostgreSQL',
    schedule_interval='@hourly',
)

def get_postgres_conn():
    return psycopg2.connect(
        host='postgres',
        dbname='airflow',
        user='airflow',
        password='airflow'
    )

def stage_songs():
    conn = get_postgres_conn()
    cur = conn.cursor()

    cur.execute("TRUNCATE TABLE staging_songs")

    base_path = '/opt/airflow/data/song_data'
    for root, _, files in os.walk(base_path):
        for file in files:
            if file.endswith('.json'):
                with open(os.path.join(root, file)) as f:
                    data = json.load(f)
                    insert_query = """
                        INSERT INTO staging_songs (
                            num_songs, artist_id, artist_name, artist_latitude,
                            artist_longitude, artist_location, song_id, title, duration, year
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """
                    cur.execute(insert_query, (
                        data.get('num_songs'),
                        data.get('artist_id'),
                        data.get('artist_name'),
                        data.get('artist_latitude'),
                        data.get('artist_longitude'),
                        data.get('artist_location'),
                        data.get('song_id'),
                        data.get('title'),
                        data.get('duration'),
                        data.get('year')
                    ))
    conn.commit()
    cur.close()
    conn.close()

def stage_events():

    conn = get_postgres_conn()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE staging_events")

    insert_query = """
        INSERT INTO staging_events (
            artist, auth, firstname, gender, iteminsession, lastname, length,
            level, location, method, page, registration, sessionid, song,
            status, ts, useragent, userid
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    base_path = '/opt/airflow/data/log_data'

    for root, _, files in os.walk(base_path):
        for file in files:
            if file.endswith('.json'):
                with open(os.path.join(root, file)) as f:
                    for line in f:
                        if line.strip() == '':
                            continue  
                        try:
                            data = json.loads(line)
                            cur.execute(insert_query, (
                                data.get('artist'),
                                data.get('auth'),
                                data.get('firstName'),
                                data.get('gender'),
                                data.get('itemInSession'),
                                data.get('lastName'),
                                data.get('length'),
                                data.get('level'),
                                data.get('location'),
                                data.get('method'),
                                data.get('page'),
                                data.get('registration'),
                                data.get('sessionId'),
                                data.get('song'),
                                data.get('status'),
                                data.get('ts'),
                                data.get('userAgent'),
                                int(data.get('userId')) if data.get('userId') not in ('', None, '') else None
                            ))
                        except Exception as e:
                            print(f"Failed to insert from {file}: {e}")
                            continue  

    conn.commit()
    cur.close()
    conn.close()

def extract_table_name(query):
    match = re.search(r'INSERT\s+INTO\s+(\w+)', query, re.IGNORECASE)
    return match.group(1) if match else None

def load_table(query, table):
    conn = get_postgres_conn()
    cur = conn.cursor()
    cur.execute(f"TRUNCATE TABLE {table}") 
    cur.execute(f"""
    INSERT INTO {table} {query}
    
    """)
 
    conn.commit()
    cur.close()
    conn.close()


def check_quality():
    conn = get_postgres_conn()
    cur = conn.cursor()

    checks = [
        # Check empty
        ("songplays", "SELECT COUNT(*) FROM songplays", lambda x: x > 0, "is not empty"),
        ("users", "SELECT COUNT(*) FROM users", lambda x: x > 0, "is not empty"),
        ("songs", "SELECT COUNT(*) FROM songs", lambda x: x > 0, "is not empty"),
        ("artists", "SELECT COUNT(*) FROM artists", lambda x: x > 0, "is not empty"),
        ("time", "SELECT COUNT(*) FROM time", lambda x: x > 0, "is not empty"),
        #Check NULL 
        ("songplays", "SELECT COUNT(*) FROM songplays WHERE playid IS NULL", lambda x: x == 0, "has no NULL playid"),
        ("users", "SELECT COUNT(*) FROM users WHERE userid IS NULL", lambda x: x == 0, "has no NULL userid"),
        ("songs", "SELECT COUNT(*) FROM songs WHERE songid IS NULL", lambda x: x == 0, "has no NULL songid"),
        ("artists", "SELECT COUNT(*) FROM artists WHERE artistid IS NULL", lambda x: x == 0, "has no NULL artistid"),
        ("time", "SELECT COUNT(*) FROM time WHERE start_time IS NULL", lambda x: x == 0, "has no NULL start_time"),
        # Check uniqueness
        ("songplays", "SELECT COUNT(playid) - COUNT(DISTINCT playid) FROM songplays", lambda x: x == 0, "has unique playid"),
        ("users", "SELECT COUNT(userid) - COUNT(DISTINCT userid) FROM users", lambda x: x == 0, "has unique userid"),
        ("songs", "SELECT COUNT(songid) - COUNT(DISTINCT songid) FROM songs", lambda x: x == 0, "has unique songid"),
        ("artists", "SELECT COUNT(artistid) - COUNT(DISTINCT artistid) FROM artists", lambda x: x == 0, "has unique artistid"),
        ("time", "SELECT COUNT(start_time) - COUNT(DISTINCT start_time) FROM time", lambda x: x == 0, "has unique start_time"),
    ]

    for table, query, condition, description in checks:
        cur.execute(query)
        result = cur.fetchone()[0]
        if not condition(result):
            raise ValueError(
                f"Data quality check failed for `{table}`: expected {description}, but query returned {result}."
            )
        print(f"{table} passed check: {description}")

    cur.close()
    conn.close()


start = DummyOperator(task_id='Begin_execution', dag=dag)

stage_songs_task = PythonOperator(
    task_id='Stage_songs',
    python_callable=stage_songs,
    dag=dag
)

stage_events_task = PythonOperator(
    task_id='Stage_events',
    python_callable=stage_events,
    dag=dag
)

load_songplays = PythonOperator(
    task_id='Load_songplays_fact_table',
    python_callable=lambda: load_table(SqlQueries.songplay_table_insert, 'songplays'),
    dag=dag
)

load_user_dim = PythonOperator(
    task_id='Load_user_dim_table',
    python_callable=lambda: load_table(SqlQueries.user_table_insert, 'users'),
    dag=dag
)

load_song_dim = PythonOperator(
    task_id='Load_song_dim_table',
    python_callable=lambda: load_table(SqlQueries.song_table_insert, 'songs'),
    dag=dag
)

load_artist_dim = PythonOperator(
    task_id='Load_artist_dim_table',
    python_callable=lambda: load_table(SqlQueries.artist_table_insert, 'artists'),
    dag=dag
)

load_time_dim = PythonOperator(
    task_id='Load_time_dim_table',
    python_callable=lambda: load_table(SqlQueries.time_table_insert, 'time'),
    dag=dag
)


quality_check = PythonOperator(
    task_id='data_quality_checks',
    python_callable=check_quality,
    dag=dag
)

end = DummyOperator(task_id='End_execution', dag=dag)

start >> [stage_songs_task, stage_events_task]

stage_songs_task >> load_songplays
stage_events_task >> load_songplays

load_songplays >> [load_user_dim, load_song_dim, load_artist_dim, load_time_dim]

[load_user_dim, load_song_dim, load_artist_dim, load_time_dim] >> quality_check >> end
