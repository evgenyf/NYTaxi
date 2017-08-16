I9E_HOME=/home/evgeny/dev/builds/IE/gigaspaces-insightedge-2.1.0-ga-100-premium
DATA_FILES_HOME=/home/evgeny/Downloads/csvs
java -cp ../target/classes:$I9E_HOME/lib/insightedge-core.jar:$I9E_HOME/jars/*:$I9E_HOME/datagrid/lib/required/*:$I9E_HOME/datagrid/lib/optional/spatial/* nyctaxi.LoadData $DATA_FILES_HOME/yellow_tripdata_2016-01.csv,$DATA_FILES_HOME/yellow_tripdata_2016-02.csv