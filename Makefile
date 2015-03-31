all:
	javac -classpath "/usr/hdp/2.2.0.0-2041/hbase/lib/*:/g/g0/achu/flume/apache-flume-1.5.2-bin/lib/*" org/apache/flume/sink/hbase/LdmsMeminfoHbaseEventSerializer.java org/apache/flume/sink/hbase/LdmsProcstatutilHbaseEventSerializer.java org/apache/flume/sink/hbase/LdmsSysclassibHbaseEventSerializer.java
	jar -cf LdmsHbaseEventSerializer.jar org/

install:
	cp LdmsHbaseEventSerializer.jar ../apache-flume-1.5.2-bin/plugins.d/custom-sink-1/lib

clean:
	rm LdmsHbaseEventSerializer.jar
	rm org/apache/flume/sink/hbase/*.class
