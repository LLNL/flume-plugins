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

public class CSVGenericHbaseEventSerializer implements HbaseEventSerializer {
    final static String clusterNameKey = "clustername";
    final static String sourceTypeKey = "sourcetype";
    final static String csvheaderKey = "csvheader";

    protected String clusterName;
    protected String sourceType;
    protected String csvheader;

    protected byte[] columnFamily;
    protected byte[] payload;

    protected String[] csvheaderfields;
    protected byte[][] csvheaderfieldsBytes;

    public CSVGenericHbaseEventSerializer(){

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

	if (!headers.containsKey(csvheader)) {
	    throw new FlumeException("Event does not contain header '" + csvheaderKey + "'");
	}

	clusterName = headers.get(clusterNameKey);
	sourceType = headers.get(sourceTypeKey);
	csvheader = headers.get(csvheaderKey);

	csvheaderfields = csvheader.split(", ");

	if (csvheaderfields.length == 0) {
	    throw new FlumeException("Event csvheader is empty");
	}

	csvheaderfieldsBytes = new byte[csvheaderfields.length][];

	for (int i = 0; i < csvheaderfields.length; i++) {
	    csvheaderfieldsBytes[i] = csvheaderfields[i].getBytes(Charsets.UTF_8);
	} 
    }


    // Basic implementation assuming the most trivial commit
    @Override
    public List<Row> getActions() throws FlumeException {
	List<Row> actions = new LinkedList<Row>();

	// Small chance this is the header
	if (payload[0] == '#') {
	    return actions;
	}

	try {
	    String payloadStr = new String(this.payload, "UTF-8");
            String[] payloadSplits = payloadStr.split(", ");

	    // csvheader check
            if (!(payloadSplits.length != csvheaderfields.length)) {
                throw new FlumeException("Number of fields in event = " + payloadSplits.length + " != fields in csv header = " + csvheaderfields.length);
            }

	    byte[] rowKey = (this.sourceType + "-" + this.clusterName + "-" + String.valueOf(System.currentTimeMillis())).getBytes("UTF8");

	    for (int i = 0; i < payloadSplits.length; i++) {
		Put put = new Put(rowKey);

		put.add(this.columnFamily, csvheaderfieldsBytes[i], payload);
		actions.add(put);
	    }
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
}
