sudo chown -R 50000:0 ./airflow/{dags,logs,plugins,config}
sudo chown 50000:0 ./airflow.cfg
sudo chmod -R 775 ./airflow/{dags,logs,plugins,config}
sudo chmod 664 ./airflow.cfg
sudo chmod -R 775 /home/phu/Pictures/Spotify-ELT-Pipeline-Analytics/airflow
