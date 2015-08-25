FLUME_VERSION = $(shell perl -ne 'print,exit if s/^\s*VERSION:\s*(\S*).*/\1/i' META)

HBASE_CLASSPATH = $(shell hbase classpath)

FLUME_CLASSPATH = "$(HBASE_CLASSPATH):/usr/hdp/current/flume-server/lib/*"

HBASE_SINK_FILES = \
	org/apache/flume/sink/hbase/CSVGenericHbaseEventSerializer.java \
	org/apache/flume/sink/hbase/LdmsCSVGenericHbaseEventSerializer.java \
	org/apache/flume/sink/hbase/LdmsHbaseEventSerializer.java \
	org/apache/flume/sink/hbase/LdmsMeminfoHbaseEventSerializer.java \
	org/apache/flume/sink/hbase/LdmsProcstatutilHbaseEventSerializer.java \
	org/apache/flume/sink/hbase/LdmsSysclassibHbaseEventSerializer.java

HBASE_SINK_JAR = HbaseEventSerializer.jar

HBASE_SINK_INSTALL_DIR = /var/lib/flume/plugins.d/hbase-sink/

INTERCEPTOR_FILES = \
	org/apache/flume/interceptor/CSVHeaderInterceptor.java

INTERCEPTOR_JAR = HbaseInterceptor.jar

INTERCEPTOR_INSTALL_DIR = /var/lib/flume/plugins.d/interceptor/

SCRIPTS_DIST = \
	scripts/flume-send-file

SCRIPTS_INSTALL_DIR = /usr/bin

EXTRA_DIST = \
	META \
	Makefile \
	flume-plugins.spec

DIST_FILES = \
	$(HBASE_SINK_FILES) \
	$(INTERCEPTOR_FILES) \
	$(SCRIPTS_DIST) \
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
	install -m 744 $(HBASE_SINK_JAR) $(DESTDIR)$(HBASE_SINK_INSTALL_DIR)
	mkdir -p $(DESTDIR)$(INTERCEPTOR_INSTALL_DIR)
	install -m 744 $(INTERCEPTOR_JAR) $(DESTDIR)$(INTERCEPTOR_INSTALL_DIR)
	mkdir -p $(DESTDIR)$(SCRIPTS_INSTALL_DIR)
	install -m 755 $(SCRIPTS_DIST) $(DESTDIR)$(SCRIPTS_INSTALL_DIR)

clean:
	rm -f *.jar
	rm -f org/apache/flume/sink/hbase/*.class
	rm -f org/apache/flume/interceptor/*.class
