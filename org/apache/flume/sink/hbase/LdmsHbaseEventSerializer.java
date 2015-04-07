package org.apache.flume.sink.hbase;

import java.io.IOException;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;

import com.google.common.base.Charsets;

public class LdmsHbaseEventSerializer implements HbaseEventSerializer {
    final static String clusterNameKey = "clustername";
    final static String sourceTypeKey = "sourcetype";

    protected String clusterName;
    protected String sourceType;

    protected byte[] columnFamily;
    protected byte[] payload;

    public LdmsHbaseEventSerializer(){

    }

    @Override
    public void configure(Context context) {
    }

    @Override
    public void configure(ComponentConfiguration conf) {
    }

    @Override
    public void initialize(Event event, byte[] columnFamily) throws FlumeException {
	this.payload = event.getBody();
	this.columnFamily = columnFamily;

	Map<String,String> headers = event.getHeaders();

	if (!headers.containsKey(clusterNameKey)) {
	    throw new FlumeException("Event does not contain header '" + clusterNameKey + "'");
	}

	if (!headers.containsKey(sourceTypeKey)) {
	    throw new FlumeException("Event does not contain header '" + sourceTypeKey + "'");
	}

	clusterName = headers.get(clusterNameKey);
	sourceType = headers.get(sourceTypeKey);
    }


    // Basic implementation assuming the most trivial commit
    @Override
    public List<Row> getActions() throws FlumeException {
	List<Row> actions = new LinkedList<Row>();
	byte[] rowKey;

	// Small chance this is the header
	if (payload[0] == '#') {
	    return actions;
	}

	try {
	    rowKey = ("ldms-" + this.sourceType + "-" + this.clusterName + "-" + String.valueOf(System.currentTimeMillis())) .getBytes("UTF8");

	    Put put = new Put(rowKey);

	    byte[] col = "LDMSPayload".getBytes(Charsets.UTF_8);
	    put.add(this.columnFamily, col, payload);
	    actions.add(put);
	} catch (Exception e) {
	    throw new FlumeException("Could not put in row!", e);
	}
	    
	return actions;
    }

    @Override
    public List<Increment> getIncrements(){
	List<Increment> incs = new LinkedList<Increment>();
	return incs;
    }

    @Override
    public void close() {
    }

    protected String calcHostname(String hostNameInt) {
	return (clusterName + String.format("%05d", Integer.parseInt(hostNameInt)));
    }

    protected byte[] calcRowkey(String hostName, String time) throws IOException {
	return ("ldms-" + this.sourceType + "-" + this.clusterName + "-" + time + "-" + hostName).getBytes("UTF8");
    }
}
