FLUME_VERSION = 1.0.0

LDMS_HBASE_SINK_FILES = \
	org/apache/flume/sink/hbase/LdmsHbaseEventSerializer.java \
	org/apache/flume/sink/hbase/LdmsMeminfoHbaseEventSerializer.java \
	org/apache/flume/sink/hbase/LdmsProcstatutilHbaseEventSerializer.java \
	org/apache/flume/sink/hbase/LdmsSysclassibHbaseEventSerializer.java

LDMS_HBASE_SINK_JAR = LdmsHbaseEventSerializer.jar

EXTRA_DIST = \
	Makefile \
	flume-plugins.spec

DIST_FILES = \
	$(LDMS_HBASE_SINK_FILES) \
	$(EXTRA_DIST)

all: ldms

ldms:
	javac -classpath "/usr/hdp/2.2.0.0-2041/hbase/lib/*:/g/g0/achu/flume/apache-flume-1.5.2-bin/lib/*" $(LDMS_HBASE_SINK_FILES)
	jar -cf $(LDMS_HBASE_SINK_JAR) org/apache/flume/sink/hbase/Ldms*.class

dist:
	rm -rf flume-plugins-$(FLUME_VERSION)
	mkdir flume-plugins-$(FLUME_VERSION)
	cp -a --parents $(DIST_FILES) flume-plugins-$(FLUME_VERSION)
	tar -czvf flume-plugins-$(FLUME_VERSION).tar.gz flume-plugins-$(FLUME_VERSION)

install:
	mkdir -p $(DESTDIR)/usr/lib/flume/plugins.d/ldms-sink/
	cp $(LDMS_HBASE_SINK_JAR) $(DESTDIR)/usr/lib/flume/plugins.d/ldms-sink

clean:
	rm *.jar
	rm org/apache/flume/sink/hbase/*.class
