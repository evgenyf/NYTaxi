1. First of all run mvn clean install on project, as result jar file [MyNYTaxi_PROJECT_LOCATION]/target/nytaxi-1.1.0-SNAPSHOT.jar will be created
2. Download InsightEdge zip file, this project now is using InsightEdge 2.1 GA for measurement purposes
3. Then go to sbin directory, define number of workers ( by using environment variable SPARK_WORKER_INSTANCES ) and run InsightEdge in demo mode: export SPARK_WORKER_INSTANCES=4 and then ./insightedge.sh --mode demo
4. gs-ui can be run in order to check that everything is running properly
5. Now submit LoadData by running following command from bin directory ( 1st parameter is list of csv file to load delimetered by comma, the 2nd parameter is using parallelism number ):
    ./insightedge-submit --class nyctaxi.LoadData --master spark://127.0.0.1:7077 [MyNYTaxi_PROJECT_LOCATION]/target/nytaxi-1.1.0-SNAPSHOT.jar [NYTaxi_CSVS_FILES_LOCATION]/yellow_tripdata_2016-01.csv 4
6. Pease verify in gs-ui space has nyctaxi.TaxiTripData objects
7. Load time result must be taken from following place in log:
    DataFrame load took 77157 msec.
8. Now submit ReadData by running following command from bin directory: ./insightedge-submit --class nyctaxi.ReadData --master spark://127.0.0.1:7077 [MyNYTaxi_PROJECT_LOCATION]/target/nytaxi-1.1.0-SNAPSHOT.jar
9. Write time result must be taken from following place in log:
    DataFrame load took 31198 msec.


# Running the jobs multiple times

Run the following from the sbin directory of insightedge
```bash
export FILES_ROOT=/disk01/datalake-data
export FILES="${FILES_ROOT1}/yellow_tripdata_2016-01.csv,${FILES_ROOT1}/yellow_tripdata_2016-02.csv,${FILES_ROOT1}/yellow_tripdata_2016-03.csv,${FILES_ROOT1}/yellow_tripdata_2016-04.csv,${FILES_ROOT1}/yellow_tripdata_2016-05.csv,${FILES_ROOT1}/yellow_tripdata_2016-06.csv,${FILES_ROOT1}/yellow_tripdata_2016-07.csv,${FILES_ROOT1}/yellow_tripdata_2016-08.csv,${FILES_ROOT1}/yellow_tripdata_2016-09.csv,${FILES_ROOT1}/yellow_tripdata_2016-10.csv,${FILES_ROOT1}/yellow_tripdata_2016-11.csv,${FILES_ROOT1}/yellow_tripdata_2016-12.csv"
export PATH_TO_JAR="~/nytaxi-1.1.0-SNAPSHOT.jar"

echo "Starting first phase"
for i in $(seq 1 10); do
	echo "Running i=${i}"
	export SPARK_WORKER_INSTANCES=4 
	killall -9 java 
	echo "Starting demo mode"
	./insightedge.sh --mode demo > /dev/null 2>&1  && echo "Sleeping..." && sleep 30s && echo "Submitting job" && ../bin/insightedge-submit --class nyctaxi.LoadData --master spark://127.0.0.1:7077 ${PATH_TO_JAR} ${FILES_1} 4 2>&1 | tee output-first${i}.out | grep "DataFrame load took" > result-first${i}.out
	sleep 10s
done

echo "Done phase one"
sleep 5s
echo "Starting second phase"

for i in $(seq 1 10); do
        echo "Running i=${i}"
        echo "Submitting job" 
        ../bin/insightedge-submit --class nyctaxi.ReadData --master spark://127.0.0.1:7077 ${PATH_TO_JAR} 2>&1 | tee output-second${i}.out | grep "DataFrame load took" > result-second${i}.out
	sleep 10s
done

echo "Done all"
```

The output of the runs can be found in output-first*.out and output-second*.out
The result of the runs can be found in result-first*.out and result-second*.out