package org.apache.flume.sink.hbase;

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

public class LdmsProcstatutilHbaseEventSerializer implements HbaseEventSerializer {
    final static String clusterNameKey = "clustername";
    final static String sourceTypeKey = "sourcetype";

    private String clusterName;
    private String sourceType;

    private byte[] columnFamily;
    private byte[] payload;

    /*
      LDMS Procstatutil Fields

      #Time, Time_usec, CompId, guest#23, steal#23, softirq#23, irq#23, iowait#23, idle#23, sys#23, nice#23, user#23, guest#22, steal#22, softirq#22, irq#22, iowait#22, idle#22, sys#22, nice#22, user#22, ..., guest#0, steal#0, softirq#0, irq#0, iowait#0, idle#0, sys#0, nice#0, user#0


    */

    // Only for columsn we're going to store, we don't use some
    // Sorta ugly, but avoid constant byte conversions later

    final static private int LDMSPROCSTATUTILINDEXHOSTNAME = 2;
    final static private int LDMSPROCSTATUTILINDEXSTART = 3;

    final static private byte[][] LDMSPROCSTATUTILCOLUMNS = {
	"LdmsProcstatutil-guest-".getBytes(Charsets.UTF_8),
	"LdmsProcstatutil-steal-".getBytes(Charsets.UTF_8),
	"LdmsProcstatutil-softirq-".getBytes(Charsets.UTF_8),
	"LdmsProcstatutil-irq-".getBytes(Charsets.UTF_8),
	"LdmsProcstatutil-iowait-".getBytes(Charsets.UTF_8),
	"LdmsProcstatutil-idle-".getBytes(Charsets.UTF_8),
	"LdmsProcstatutil-sys-".getBytes(Charsets.UTF_8),
	"LdmsProcstatutil-nice-".getBytes(Charsets.UTF_8),
	"LdmsProcstatutil-user-".getBytes(Charsets.UTF_8),
    };

    public LdmsProcstatutilHbaseEventSerializer(){

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

    @Override
    public List<Row> getActions() throws FlumeException {
	List<Row> actions = new LinkedList<Row>();
	byte[] rowKey;

	// Small chance this is the header
	if (this.payload[0] == '#') {
	    return actions;
	}

	try {
	    String payloadStr = new String(payload, "UTF-8");
	    String[] payloadSplits = payloadStr.split(", ");

	    if (!(payloadSplits.length >= LDMSPROCSTATUTILINDEXSTART)) {
		throw new FlumeException("Invalid number of payload splits " + payloadSplits.length);
	    }

	    if (((payloadSplits.length - LDMSPROCSTATUTILINDEXSTART) % LDMSPROCSTATUTILCOLUMNS.length) != 0) {
		throw new FlumeException("Invalid number of payload splits " + payloadSplits.length);
	    }

	    int numcpus = (payloadSplits.length - LDMSPROCSTATUTILINDEXSTART) / LDMSPROCSTATUTILCOLUMNS.length;

	    String hostName = clusterName + String.format("%05d", Integer.parseInt(payloadSplits[LDMSPROCSTATUTILINDEXHOSTNAME]));
	    
	    rowKey = ("ldms-" + clusterName + "-" + hostName + "-" + sourceType + "-" + String.valueOf(System.currentTimeMillis())).getBytes("UTF8");

	    for (int i = 0; i < numcpus; i++) {
		byte[] cpunum = String.format("%02d", numcpus - i - 1).getBytes(Charsets.UTF_8);

		for (int j = 0; j < LDMSPROCSTATUTILCOLUMNS.length; j++) {
		    Put put = new Put(rowKey);
		    
		    byte[] val = payloadSplits[LDMSPROCSTATUTILINDEXSTART + i*LDMSPROCSTATUTILCOLUMNS.length + j].getBytes(Charsets.UTF_8);
		    // This is probably really slow, any way to do this faster?
		    // could calculate max field length and create buffer earlier, deal w/ later
		    byte[] col = new byte[LDMSPROCSTATUTILCOLUMNS[j].length + cpunum.length];
		    System.arraycopy(LDMSPROCSTATUTILCOLUMNS[j], 0, col, 0, LDMSPROCSTATUTILCOLUMNS[j].length);
		    System.arraycopy(cpunum, 0, col, LDMSPROCSTATUTILCOLUMNS[j].length, cpunum.length);
		    put.add(columnFamily, col, val);
		    actions.add(put);
		}
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
