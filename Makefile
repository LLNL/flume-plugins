FLUME_VERSION = $(shell perl -ne 'print,exit if s/^\s*VERSION:\s*(\S*).*/\1/i' META)

HBASE_SINK_FILES = \
	org/apache/flume/sink/hbase/LdmsHbaseEventSerializer.java \
	org/apache/flume/sink/hbase/LdmsMeminfoHbaseEventSerializer.java \
	org/apache/flume/sink/hbase/LdmsProcstatutilHbaseEventSerializer.java \
	org/apache/flume/sink/hbase/LdmsSysclassibHbaseEventSerializer.java

HBASE_SINK_JAR = HbaseEventSerializer.jar

HBASE_SINK_INSTALL_DIR = /usr/hdp/2.2.4.2-2/flume/plugins.d/hbase-sink/

EXTRA_DIST = \
	META \
	Makefile \
	flume-plugins.spec

DIST_FILES = \
	$(HBASE_SINK_FILES) \
	$(EXTRA_DIST)

all: hbasesink

hbasesink:
	javac -classpath "/usr/hdp/2.2.0.0-2041/hbase/lib/*:/usr/hdp/2.2.4.2-2/flume/lib/*" $(HBASE_SINK_FILES)
	jar -cf $(HBASE_SINK_JAR) org/apache/flume/sink/hbase/*.class

dist:
	rm -rf flume-plugins-$(FLUME_VERSION)
	mkdir flume-plugins-$(FLUME_VERSION)
	cp -a --parents $(DIST_FILES) flume-plugins-$(FLUME_VERSION)
	tar -czvf flume-plugins-$(FLUME_VERSION).tar.gz flume-plugins-$(FLUME_VERSION)

install:
	mkdir -p $(DESTDIR)$(HBASE_SINK_INSTALL_DIR)
	cp $(HBASE_SINK_JAR) $(DESTDIR)$(HBASE_SINK_INSTALL_DIR)

clean:
	rm *.jar
	rm org/apache/flume/sink/hbase/*.class
