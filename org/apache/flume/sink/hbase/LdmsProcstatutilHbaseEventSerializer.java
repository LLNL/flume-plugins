/*****************************************************************************\
 * Copyright (c) 2015, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory
 * Written by Albert Chu <chu11@llnl.gov>
 * LLNL-CODE-673778
 * All rights reserved.
 * This file is part of flume-plugins. 
 *
 * For details, see https://github.com/chu11/flume-plugins. Please also
 * read project DISCLAIMER
 * (https://github.com/chu11/flume-plugins/blob/master/DISCLAIMER).
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * - Redistributions of source code must retain the above copyright
 *   notice, this list of conditions and the disclaimer below.
 * - Redistributions in binary form must reproduce the above copyright
 *   notice, this list of conditions and the disclaimer (as noted below)
 *   in the documentation and/or other materials provided with the
 *   distribution.
 * - Neither the name of the LLNS/LLNL nor the names of its contributors
 *   may be used to endorse or promote products derived from this
 *   software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL LAWRENCE
 * LIVERMORE NATIONAL SECURITY, LLC, THE U.S. DEPARTMENT OF ENERGY OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
\*****************************************************************************/
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
