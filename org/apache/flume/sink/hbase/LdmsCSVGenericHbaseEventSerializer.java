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

public class LdmsCSVGenericHbaseEventSerializer extends LdmsHbaseEventSerializer {
    final static String csvheaderKey = "csvheader";

    protected String csvheader;

    protected String[] csvheaderfields;
    protected byte[][] csvheaderfieldsBytes;

    public LdmsCSVGenericHbaseEventSerializer(){

    }

    @Override
    public void initialize(Event event, byte[] columnFamily) throws FlumeException {
	super.initialize(event, columnFamily);

	Map<String,String> headers = event.getHeaders();

	if (!headers.containsKey(csvheader)) {
	    throw new FlumeException("Event does not contain header '" + csvheaderKey + "'");
	}

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

            byte[] rowKey = calcRowkey(calcHostname(payloadSplits[LDMS_INDEX_HOSTNAME]),
                                       calcTimestamp(payloadSplits[LDMS_INDEX_TIME]));

	    for (int i = LDMS_INDEX_FIRST_DATA; i < payloadSplits.length; i++) {
		Put put = new Put(rowKey);

		byte[] val = payloadSplits[i].getBytes(Charsets.UTF_8);
                put.add(this.columnFamily, csvheaderfieldsBytes[i], val);
		actions.add(put);
	    }
	} catch (Exception e) {
	    throw new FlumeException("Could not put in row!", e);
	}
	    
	return actions;
    }
}
