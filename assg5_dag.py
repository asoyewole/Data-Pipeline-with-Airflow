from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator



default_args = {
    'owner': 'Ayotunde',
    'dag_id': 'traffic_data_ETL',
    'start_date':datetime(2024, 1, 30),
    'schedule': '@daily',
    'retries': 1,
    'retry_delay': timedelta(minutes = 3),
    }




dag = DAG('assg5',
          default_args=default_args,
          description = 'dag for assignment 5, UW PACE PA data acquisition and management class',
          schedule=timedelta(days=1),
          start_date=datetime(2024, 1, 29),
          catchup=False,
        )




task1 = BashOperator(
    task_id = 'create_directory',
    # Create complete 'directory structure'
    bash_command= '''mkdir -p ~/Documents/assignment_5 ~/Documents/assignment_5/zipped_file ~/Documents/assignment_5/unzipped_files ~/Documents/assignment_5/data_extract ~/Documents/assignment_5/merged_data ~/Documents/assignment_5/transformed_data''',
    dag = dag
)



URL = 'https://elasticbeanstalk-us-east-2-340729127361.s3.us-east-2.amazonaws.com/trafficdata.tgz'

task2 = BashOperator(
    task_id = 'download',
    # f-string the url into the bash command
    bash_command = f'wget -P ~/Documents/assignment_5/zipped_file {URL}',
    dag = dag
)


task3 = BashOperator(
    task_id = 'unzip',
    #unzip the downloaded data into assignment_5 directory
    bash_command= 'tar zxf ~/Documents/assignment_5/zipped_file/trafficdata.tgz -C ~/Documents/assignment_5/unzipped_files',
    dag = dag
)


task4 = BashOperator(
    task_id = 'csv_extractor',
    bash_command = "cut -d ',' -f 1,2,3,4 ~/Documents/assignment_5/unzipped_files/vehicle-data.csv > ~/Documents/assignment_5/data_extract/csv_d.csv",
    dag = dag
)

task5 = BashOperator(
    task_id = 'tsv_extractor',
    # cut columns 5,6,7 and save to temporary folder with name tsv.csv
    bash_command= "cut -f 5,6,7 ~/Documents/assignment_5/unzipped_files/tollplaza-data.tsv | tr '\t' ',' > ~/Documents/assignment_5/data_extract/tsv_d.csv",
    dag = dag
)


task6 = BashOperator(
    task_id = 'txt_extractor',
    bash_command = "awk '{print $10\",\"$11}' ~/Documents/assignment_5/unzipped_files/payment-data.txt > ~/Documents/assignment_5/data_extract/fixed_width_d.csv",
    dag = dag
)


task7 = BashOperator(
    task_id = 'csv_merger',
    bash_command= "paste -d ',' ~/Documents/assignment_5/data_extract/csv_d.csv ~/Documents/assignment_5/data_extract/tsv_d.csv ~/Documents/assignment_5/data_extract/fixed_width_d.csv > ~/Documents/assignment_5/merged_data/merged_d.csv",
    dag = dag
)



task8 = BashOperator(
    task_id = 'transformer',
    bash_command= '''awk 'BEGIN {FS=OFS=","} {print $1,$2,$3,toupper($4),$5,$6,$7,$8,$9}' ~/Documents/assignment_5/merged_data/merged_d.csv > ~/Documents/assignment_5/transformed_data/tranformed.csv''',
    # bash_command= "awk '{print $1, $2, $3, toupper($4), $5, $6, $7, $8, $9}' ~/Documents/assignment_5/merged_data/merged_d.csv > ~/Documents/assignment_5/transformed_data/tranformed.csv",
    #bash_command = "sed 's/[a-z]/\U&/4' ~/Documents/assignment_5/transformed_data/merged_data/merged_d.csv > ~/Documents/assignment_5/transformed_data/tranformed.csv",
    dag = dag
)



task1>>task2>>task3>>[task4,task5,task6]>>task7>>task8