package org.apache.flume.sink.hbase;

import java.util.LinkedList;
import java.util.List;

import org.apache.flume.FlumeException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;

import com.google.common.base.Charsets;

public class LdmsProcstatutilHbaseEventSerializer extends LdmsHbaseEventSerializer {
    /*
      LDMS Procstatutil Fields

      #Time, Time_usec, CompId, guest#23, steal#23, softirq#23, irq#23, iowait#23, idle#23, sys#23, nice#23, user#23, guest#22, steal#22, softirq#22, irq#22, iowait#22, idle#22, sys#22, nice#22, user#22, ..., guest#0, steal#0, softirq#0, irq#0, iowait#0, idle#0, sys#0, nice#0, user#0


    */

    // Only for columsn we're going to store, we don't use some
    // Sorta ugly, but avoid constant byte conversions later

    final static private byte[][] LDMS_PROCSTATUTIL_COLUMNS = {
	"guest-".getBytes(Charsets.UTF_8),
	"steal-".getBytes(Charsets.UTF_8),
	"softirq-".getBytes(Charsets.UTF_8),
	"irq-".getBytes(Charsets.UTF_8),
	"iowait-".getBytes(Charsets.UTF_8),
	"idle-".getBytes(Charsets.UTF_8),
	"sys-".getBytes(Charsets.UTF_8),
	"nice-".getBytes(Charsets.UTF_8),
	"user-".getBytes(Charsets.UTF_8),
    };

    public LdmsProcstatutilHbaseEventSerializer() {

    }

    @Override
    public List<Row> getActions() throws FlumeException {
	List<Row> actions = new LinkedList<Row>();

	if (payloadValid() == false) {
	    return actions;
	}

	try {
	    String payloadStr = new String(this.payload, "UTF-8");
	    String[] payloadSplits = payloadStr.split(", ");

	    if (!(payloadSplits.length >= LDMS_INDEX_FIRST_DATA)) {
		throw new FlumeException("Invalid number of payload splits " + payloadSplits.length);
	    }

	    if (((payloadSplits.length - LDMS_INDEX_FIRST_DATA) % LDMS_PROCSTATUTIL_COLUMNS.length) != 0) {
		throw new FlumeException("Invalid number of payload splits " + payloadSplits.length);
	    }

	    int numcpus = (payloadSplits.length - LDMS_INDEX_FIRST_DATA) / LDMS_PROCSTATUTIL_COLUMNS.length;

	    byte[] rowKey = calcRowkey(calcHostname(payloadSplits[LDMS_INDEX_HOSTNAME]),
				       calcTimestamp(payloadSplits[LDMS_INDEX_TIME]));

	    for (int i = 0; i < numcpus; i++) {
		byte[] cpunum = String.format("%02d", numcpus - i - 1).getBytes(Charsets.UTF_8);

		for (int j = 0; j < LDMS_PROCSTATUTIL_COLUMNS.length; j++) {
		    Put put = new Put(rowKey);
		    
		    byte[] val = payloadSplits[LDMS_INDEX_FIRST_DATA + i*LDMS_PROCSTATUTIL_COLUMNS.length + j].getBytes(Charsets.UTF_8);
		    // This is probably really slow, any way to do this faster?
		    // could calculate max field length and create buffer earlier, deal w/ later
		    byte[] col = new byte[LDMS_PROCSTATUTIL_COLUMNS[j].length + cpunum.length];
		    System.arraycopy(LDMS_PROCSTATUTIL_COLUMNS[j], 0, col, 0, LDMS_PROCSTATUTIL_COLUMNS[j].length);
		    System.arraycopy(cpunum, 0, col, LDMS_PROCSTATUTIL_COLUMNS[j].length, cpunum.length);
		    put.add(this.columnFamily, col, val);
		    actions.add(put);
		}
	    }
	} catch (Exception e) {
	    throw new FlumeException("Could not put in row!", e);
	}
	    
	return actions;
    }
}
