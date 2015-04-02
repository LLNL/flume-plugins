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

public class LdmsSysclassibHbaseEventSerializer implements HbaseEventSerializer {
    final static String clusterNameKey = "clustername";
    final static String sourceTypeKey = "sourcetype";

    private String clusterName;
    private String sourceType;

    private byte[] columnFamily;
    private byte[] payload;

    /*
      LDMS Sysclassib Fields

      #Time, Time_usec, CompId, ib.port_multicast_rcv_packets#qib0.1, ib.port_multicast_xmit_packets#qib0.1, ib.port_unicast_rcv_packets#qib0.1, ib.port_unicast_xmit_packets#qib0.1, ib.port_xmit_wait#qib0.1, ib.port_rcv_packets#qib0.1, ib.port_xmit_packets#qib0.1, ib.port_rcv_data#qib0.1, ib.port_xmit_data#qib0.1, ib.VL15_dropped#qib0.1, ib.excessive_buffer_overrun_errors#qib0.1, ib.local_link_integrity_errors#qib0.1, ib.COUNTER_SELECT2_F#qib0.1, ib.port_rcv_constraint_errors#qib0.1, ib.port_xmit_constraint_errors#qib0.1, ib.port_xmit_discards#qib0.1, ib.port_rcv_switch_relay_errors#qib0.1, ib.port_rcv_remote_physical_errors#qib0.1, ib.port_rcv_errors#qib0.1, ib.link_downed#qib0.1, ib.link_error_recovery#qib0.1, ib.symbol_error#qib0.1, ib.port_multicast_rcv_packets#qib1.1, ib.port_multicast_xmit_packets#qib1.1, ib.port_unicast_rcv_packets#qib1.1, ib.port_unicast_xmit_packets#qib1.1, ib.port_xmit_wait#qib1.1, ib.port_rcv_packets#qib1.1, ib.port_xmit_packets#qib1.1, ib.port_rcv_data#qib1.1, ib.port_xmit_data#qib1.1, ib.VL15_dropped#qib1.1, ib.excessive_buffer_overrun_errors#qib1.1, ib.local_link_integrity_errors#qib1.1, ib.COUNTER_SELECT2_F#qib1.1, ib.port_rcv_constraint_errors#qib1.1, ib.port_xmit_constraint_errors#qib1.1, ib.port_xmit_discards#qib1.1, ib.port_rcv_switch_relay_errors#qib1.1, ib.port_rcv_remote_physical_errors#qib1.1, ib.port_rcv_errors#qib1.1, ib.link_downed#qib1.1, ib.link_error_recovery#qib1.1, ib.symbol_error#qib1.1

    */

    // Only for columsn we're going to store, we don't use some
    // Sorta ugly, but avoid constant byte conversions later

    final static private int LDMSSYSCLASSIBINDEXHOSTNAME = 2;
    final static private int LDMSSYSCLASSIBINDEXSTART = 3;

    final static private byte[][] LDMSSYSCLASSIBCOLUMNS = {
	"LdmsSysclassib-ib.port_multicast_rcv_packets-".getBytes(Charsets.UTF_8),
	"LdmsSysclassib-ib.port_multicast_xmit_packets-".getBytes(Charsets.UTF_8),
	"LdmsSysclassib-ib.port_unicast_rcv_packets-".getBytes(Charsets.UTF_8),
	"LdmsSysclassib-ib.port_unicast_xmit_packets-".getBytes(Charsets.UTF_8),
	"LdmsSysclassib-ib.port_xmit_wait-".getBytes(Charsets.UTF_8),
	"LdmsSysclassib-ib.port_rcv_packets-".getBytes(Charsets.UTF_8),
	"LdmsSysclassib-ib.port_xmit_packets-".getBytes(Charsets.UTF_8),
	"LdmsSysclassib-ib.port_rcv_data-".getBytes(Charsets.UTF_8),
	"LdmsSysclassib-ib.port_xmit_data-".getBytes(Charsets.UTF_8),
	"LdmsSysclassib-ib.VL15_dropped-".getBytes(Charsets.UTF_8),
	"LdmsSysclassib-ib.excessive_buffer_overrun_errors-".getBytes(Charsets.UTF_8),
	"LdmsSysclassib-ib.local_link_integrity_errors-".getBytes(Charsets.UTF_8),
	"LdmsSysclassib-ib.COUNTER_SELECT2_F-".getBytes(Charsets.UTF_8),
	"LdmsSysclassib-ib.port_rcv_constraint_errors-".getBytes(Charsets.UTF_8),
	"LdmsSysclassib-ib.port_xmit_constraint_errors-".getBytes(Charsets.UTF_8),
	"LdmsSysclassib-ib.port_xmit_discards-".getBytes(Charsets.UTF_8),
	"LdmsSysclassib-ib.port_rcv_switch_relay_errors-".getBytes(Charsets.UTF_8),
	"LdmsSysclassib-ib.port_rcv_remote_physical_errors-".getBytes(Charsets.UTF_8),
	"LdmsSysclassib-ib.port_rcv_errors-".getBytes(Charsets.UTF_8),
	"LdmsSysclassib-ib.link_downed-".getBytes(Charsets.UTF_8),
	"LdmsSysclassib-ib.link_error_recovery-".getBytes(Charsets.UTF_8),
	"LdmsSysclassib-ib.symbol_error-".getBytes(Charsets.UTF_8),
    };

    public LdmsSysclassibHbaseEventSerializer(){

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

	    if (!(payloadSplits.length >= LDMSSYSCLASSIBINDEXSTART)) {
		throw new FlumeException("Invalid number of payload splits " + payloadSplits.length);
	    }

	    if (((payloadSplits.length - LDMSSYSCLASSIBINDEXSTART) % LDMSSYSCLASSIBCOLUMNS.length) != 0) {
		throw new FlumeException("Invalid number of payload splits " + payloadSplits.length);
	    }

	    // This will not work for each cluster, don't know card or ib port, see LCMON-12

	    int ibcards = (payloadSplits.length - LDMSSYSCLASSIBINDEXSTART) / LDMSSYSCLASSIBCOLUMNS.length;

	    String hostName = clusterName + String.format("%05d", Integer.parseInt(payloadSplits[LDMSSYSCLASSIBINDEXHOSTNAME]));
	    
	    rowKey = ("ldms-" + sourceType + "-" + clusterName + "-" + String.valueOf(System.currentTimeMillis())).getBytes("UTF8");

	    for (int i = 0; i < ibcards; i++) {
		byte[] ibcard = Integer.toString(ibcards - i - 1).getBytes(Charsets.UTF_8);

		for (int j = 0; j < LDMSSYSCLASSIBCOLUMNS.length; j++) {
		    Put put = new Put(rowKey);
		    
		    byte[] val = payloadSplits[LDMSSYSCLASSIBINDEXSTART + i*LDMSSYSCLASSIBCOLUMNS.length + j].getBytes(Charsets.UTF_8);
		    // This is probably really slow, any way to do this faster?
		    // could calculate max field length and create buffer earlier, deal w/ later
		    byte[] col = new byte[LDMSSYSCLASSIBCOLUMNS[j].length + ibcard.length];
		    System.arraycopy(LDMSSYSCLASSIBCOLUMNS[j], 0, col, 0, LDMSSYSCLASSIBCOLUMNS[j].length);
		    System.arraycopy(ibcard, 0, col, LDMSSYSCLASSIBCOLUMNS[j].length, ibcard.length);
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
