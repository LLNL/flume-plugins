FLUME_VERSION = $(shell perl -ne 'print,exit if s/^\s*VERSION:\s*(\S*).*/\1/i' META)

FLUME_CLASSPATH = "/usr/hdp/2.2.0.0-2041/hbase/lib/*:/usr/hdp/2.2.4.2-2/flume/lib/*"

HBASE_SINK_FILES = \
	org/apache/flume/sink/hbase/LdmsCSVGenericHbaseEventSerializer.java \
	org/apache/flume/sink/hbase/LdmsHbaseEventSerializer.java \
	org/apache/flume/sink/hbase/LdmsMeminfoHbaseEventSerializer.java \
	org/apache/flume/sink/hbase/LdmsProcstatutilHbaseEventSerializer.java \
	org/apache/flume/sink/hbase/LdmsSysclassibHbaseEventSerializer.java

HBASE_SINK_JAR = HbaseEventSerializer.jar

HBASE_SINK_INSTALL_DIR = /usr/hdp/2.2.4.2-2/flume/plugins.d/hbase-sink/

INTERCEPTOR_FILES = \
	org/apache/flume/interceptor/CSVHeaderInterceptor.java

INTERCEPTOR_JAR = HbaseInterceptor.jar

INTERCEPTOR_INSTALL_DIR = /usr/hdp/2.2.4.2-2/flume/plugins.d/interceptor/

EXTRA_DIST = \
	META \
	Makefile \
	flume-plugins.spec

DIST_FILES = \
	$(HBASE_SINK_FILES) \
	$(INTERCEPTOR_FILES) \
	$(EXTRA_DIST)

all: hbasesink interceptor

hbasesink:
	javac -classpath $(FLUME_CLASSPATH) $(HBASE_SINK_FILES)
	jar -cf $(HBASE_SINK_JAR) org/apache/flume/sink/hbase/*.class

interceptor:
	javac -classpath $(FLUME_CLASSPATH) $(INTERCEPTOR_FILES)
	jar -cf $(INTERCEPTOR_JAR) org/apache/flume/interceptor/*.class

dist:
	rm -rf flume-plugins-$(FLUME_VERSION)
	mkdir flume-plugins-$(FLUME_VERSION)
	cp -a --parents $(DIST_FILES) flume-plugins-$(FLUME_VERSION)
	tar -czvf flume-plugins-$(FLUME_VERSION).tar.gz flume-plugins-$(FLUME_VERSION)

install:
	mkdir -p $(DESTDIR)$(HBASE_SINK_INSTALL_DIR)
	cp $(HBASE_SINK_JAR) $(DESTDIR)$(HBASE_SINK_INSTALL_DIR)
	mkdir -p $(DESTDIR)$(INTERCEPTOR_INSTALL_DIR)
	cp $(INTERCEPTOR_JAR) $(DESTDIR)$(INTERCEPTOR_INSTALL_DIR)

clean:
	rm *.jar
	rm org/apache/flume/sink/hbase/*.class
	rm org/apache/flume/interceptor/*.class
