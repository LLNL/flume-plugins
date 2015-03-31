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

public class LdmsMeminfoHbaseEventSerializer implements HbaseEventSerializer {
    final static String clusterNameKey = "clustername";
    final static String sourceTypeKey = "sourcetype";

    private String clusterName;
    private String sourceType;

    private byte[] columnFamily;
    private byte[] payload;

    /*
      LDMS MemInfo Fields

      #Time, Time_usec, CompId, DirectMap1G, DirectMap2M, DirectMap4k, Hugepagesize, HugePages_Surp, HugePages_Rsvd, HugePages_Free, HugePages_Total, AnonHugePages, HardwareCorrupted, VmallocChunk, VmallocUsed, VmallocTotal, Committed_AS, CommitLimit, WritebackTmp, Bounce, NFS_Unstable, PageTables, KernelStack, SUnreclaim, SReclaimable, Slab, Shmem, Mapped, AnonPages, Writeback, Dirty, SwapFree, SwapTotal, Mlocked, Unevictable, Inactive(file), Active(file), Inactive(anon), Active(anon), Inactive, Active, SwapCached, Cached, Buffers, MemFree, MemTotal

    */

    final static private int LDMSMEMINFOLENGTH = 46;

    // Only for columsn we're going to store, we don't use some
    // Sorta ugly, but avoid constant byte conversions later

    final static private int LDMSMEMINFOINDEXHOSTNAME = 2;
    final static private int LDMSMEMINFOINDEXSTART = 3;

    final static private byte[][] LDMSMEMINFOCOLUMNS = {
	"LdmsMeminfo-DirectMap1G".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-DirectMap2M".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-DirectMap4k".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-Hugepagesize".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-HugePages_Surp".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-HugePages_Rsvd".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-HugePages_Free".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-HugePages_Total".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-AnonHugePages".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-HardwareCorrupted".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-VmallocChunk".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-VmallocUsed".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-VmallocTotal".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-Committed_AS".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-CommitLimit".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-WritebackTmp".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-Bounce".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-NFS_Unstable".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-PageTables".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-KernelStack".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-SUnreclaim".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-SReclaimable".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-Slab".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-Shmem".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-Mapped".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-AnonPages".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-Writeback".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-Dirty".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-SwapFree".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-SwapTotal".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-Mlocked".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-Unevictable".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-Inactive_file".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-Active_file".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-Inactive_anon".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-Active_anon".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-Inactive".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-Active".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-SwapCached".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-Cached".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-Buffers".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-MemFree".getBytes(Charsets.UTF_8),
	"LdmsMeminfo-MemTotal".getBytes(Charsets.UTF_8),
    };

    public LdmsMeminfoHbaseEventSerializer(){

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

	    if (payloadSplits.length != LDMSMEMINFOLENGTH) {
		throw new FlumeException("Invalid number of payload splits " + payloadSplits.length);
	    }

	    String hostName = clusterName + payloadSplits[LDMSMEMINFOINDEXHOSTNAME];
	    
	    rowKey = ("ldms-" + clusterName + "-" + hostName + "-" + sourceType + "-" + String.valueOf(System.currentTimeMillis())).getBytes("UTF8");

	    for (int i = 0; i < LDMSMEMINFOCOLUMNS.length; i++) {
		Put put = new Put(rowKey);

		byte[] val = payloadSplits[LDMSMEMINFOINDEXSTART + i].getBytes(Charsets.UTF_8);
		put.add(columnFamily, LDMSMEMINFOCOLUMNS[i], val);
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
