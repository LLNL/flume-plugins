package org.apache.flume.sink.hbase;

import java.util.LinkedList;
import java.util.List;

import org.apache.flume.FlumeException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;

import com.google.common.base.Charsets;

public class LdmsMeminfoHbaseEventSerializer extends LdmsHbaseEventSerializer {
    /*
      LDMS MemInfo Fields

      #Time, Time_usec, CompId, DirectMap1G, DirectMap2M, DirectMap4k, Hugepagesize, HugePages_Surp, HugePages_Rsvd, HugePages_Free, HugePages_Total, AnonHugePages, HardwareCorrupted, VmallocChunk, VmallocUsed, VmallocTotal, Committed_AS, CommitLimit, WritebackTmp, Bounce, NFS_Unstable, PageTables, KernelStack, SUnreclaim, SReclaimable, Slab, Shmem, Mapped, AnonPages, Writeback, Dirty, SwapFree, SwapTotal, Mlocked, Unevictable, Inactive(file), Active(file), Inactive(anon), Active(anon), Inactive, Active, SwapCached, Cached, Buffers, MemFree, MemTotal

    */

    final static private int LDMS_MEMINFO_LENGTH = 46;

    // Only for columsn we're going to store, we don't use some
    // Sorta ugly, but avoid constant byte conversions later

    final static private byte[][] LDMS_MEMINFO_COLUMNS = {
	"DirectMap1G".getBytes(Charsets.UTF_8),
	"DirectMap2M".getBytes(Charsets.UTF_8),
	"DirectMap4k".getBytes(Charsets.UTF_8),
	"Hugepagesize".getBytes(Charsets.UTF_8),
	"HugePages_Surp".getBytes(Charsets.UTF_8),
	"HugePages_Rsvd".getBytes(Charsets.UTF_8),
	"HugePages_Free".getBytes(Charsets.UTF_8),
	"HugePages_Total".getBytes(Charsets.UTF_8),
	"AnonHugePages".getBytes(Charsets.UTF_8),
	"HardwareCorrupted".getBytes(Charsets.UTF_8),
	"VmallocChunk".getBytes(Charsets.UTF_8),
	"VmallocUsed".getBytes(Charsets.UTF_8),
	"VmallocTotal".getBytes(Charsets.UTF_8),
	"Committed_AS".getBytes(Charsets.UTF_8),
	"CommitLimit".getBytes(Charsets.UTF_8),
	"WritebackTmp".getBytes(Charsets.UTF_8),
	"Bounce".getBytes(Charsets.UTF_8),
	"NFS_Unstable".getBytes(Charsets.UTF_8),
	"PageTables".getBytes(Charsets.UTF_8),
	"KernelStack".getBytes(Charsets.UTF_8),
	"SUnreclaim".getBytes(Charsets.UTF_8),
	"SReclaimable".getBytes(Charsets.UTF_8),
	"Slab".getBytes(Charsets.UTF_8),
	"Shmem".getBytes(Charsets.UTF_8),
	"Mapped".getBytes(Charsets.UTF_8),
	"AnonPages".getBytes(Charsets.UTF_8),
	"Writeback".getBytes(Charsets.UTF_8),
	"Dirty".getBytes(Charsets.UTF_8),
	"SwapFree".getBytes(Charsets.UTF_8),
	"SwapTotal".getBytes(Charsets.UTF_8),
	"Mlocked".getBytes(Charsets.UTF_8),
	"Unevictable".getBytes(Charsets.UTF_8),
	"Inactive_file".getBytes(Charsets.UTF_8),
	"Active_file".getBytes(Charsets.UTF_8),
	"Inactive_anon".getBytes(Charsets.UTF_8),
	"Active_anon".getBytes(Charsets.UTF_8),
	"Inactive".getBytes(Charsets.UTF_8),
	"Active".getBytes(Charsets.UTF_8),
	"SwapCached".getBytes(Charsets.UTF_8),
	"Cached".getBytes(Charsets.UTF_8),
	"Buffers".getBytes(Charsets.UTF_8),
	"MemFree".getBytes(Charsets.UTF_8),
	"MemTotal".getBytes(Charsets.UTF_8),
    };

    public LdmsMeminfoHbaseEventSerializer(){

    }

    @Override
    public List<Row> getActions() throws FlumeException {
	List<Row> actions = new LinkedList<Row>();

	// Small chance this is the header
	if (this.payload[0] == '#') {
	    return actions;
	}

	try {
	    String payloadStr = new String(this.payload, "UTF-8");
	    String[] payloadSplits = payloadStr.split(", ");

	    if (payloadSplits.length != LDMS_MEMINFO_LENGTH) {
		throw new FlumeException("Invalid number of payload splits " + payloadSplits.length);
	    }

	    byte[] rowKey = calcRowkey(calcHostname(payloadSplits[LDMS_INDEX_HOSTNAME]),
				       calcTimestamp(payloadSplits[LDMS_INDEX_TIME]));

	    for (int i = 0; i < LDMS_MEMINFO_COLUMNS.length; i++) {
		Put put = new Put(rowKey);

		byte[] val = payloadSplits[LDMS_INDEX_FIRST_DATA + i].getBytes(Charsets.UTF_8);
		put.add(this.columnFamily, LDMS_MEMINFO_COLUMNS[i], val);
		actions.add(put);
	    }
	} catch (Exception e) {
	    throw new FlumeException("Could not put in row!", e);
	}
	    
	return actions;
    }
}
