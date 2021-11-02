Steps to import the wordcount output in hbase table using happybase:

Hadoop version -3.3.1 and Hbase version -1.4.6


1. Download happybase

	Pip3 install happybase


2. Along with hadoop and hbase daemons start happybase daemon

	hbase-daemon.sh start thrift


3. Get wordcount output from hadoop

	hadoop fs -get /WordCountHadoop/Output /home/samarth/Document


4. Write the python code for importing output to hbase table.


5. Also scan the table in hbase shell to check whether data is imported.


