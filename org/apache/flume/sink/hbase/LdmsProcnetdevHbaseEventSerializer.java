/*****************************************************************************\
 * Copyright (c) 2015, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory
 * Written by Albert Chu <chu11@llnl.gov>
 * LLNL-CODE-673778
 * All rights reserved.
 * This file is part of flume-plugins. 
 *
 * For details, see https://github.com/llnl/flume-plugins. Please also
 * read project DISCLAIMER
 * (https://github.com/llnl/flume-plugins/blob/master/DISCLAIMER).
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

public class LdmsProcnetdevHbaseEventSerializer extends LdmsHbaseEventSerializer {
    /*
      LDMS Procnetdev Fields

      #Time, Time_usec, CompId, tx_compressed#ib0, tx_carrier#ib0, tx_colls#ib0, tx_fifo#ib0, tx_drop#ib0, tx_errs#ib0, tx_packets#ib0, tx_bytes#ib0, rx_multicast#ib0, rx_compressed#ib0, rx_frame#ib0, rx_fifo#ib0, rx_drop#ib0, rx_errs#ib0, rx_packets#ib0, rx_bytes#ib0

    */

    // Only for columsn we're going to store, we don't use some
    // Sorta ugly, but avoid constant byte conversions later

    final static private byte[][] LDMS_PROCNETDEV_COLUMNS = {
	"tx_compressed-".getBytes(Charsets.UTF_8),
	"tx_carrier-".getBytes(Charsets.UTF_8),
	"tx_colls-".getBytes(Charsets.UTF_8),
	"tx_fifo-".getBytes(Charsets.UTF_8),
	"tx_drop-".getBytes(Charsets.UTF_8),
	"tx_errs-".getBytes(Charsets.UTF_8),
	"tx_packets-".getBytes(Charsets.UTF_8),
	"tx_bytes-".getBytes(Charsets.UTF_8),
	"rx_multicast-".getBytes(Charsets.UTF_8),
	"rx_compressed-".getBytes(Charsets.UTF_8),
	"rx_frame-".getBytes(Charsets.UTF_8),
	"rx_fifo-".getBytes(Charsets.UTF_8),
	"rx_drop-".getBytes(Charsets.UTF_8),
	"rx_errs-".getBytes(Charsets.UTF_8),
	"rx_packets-".getBytes(Charsets.UTF_8),
	"rx_bytes-".getBytes(Charsets.UTF_8),
    };

    public LdmsProcnetdevHbaseEventSerializer() {

    }

    @Override
    public List<Row> getActions() throws FlumeException {
	List<Row> actions = new LinkedList<Row>();

	if (payloadValid() == false) {
	    return actions;
	}

	try {
	    String payloadStr = new String(this.payload, "UTF-8");
	    String[] payloadSplits = payloadStr.split("\\s*[, ]\\s*");

	    if (!(payloadSplits.length >= LDMS_INDEX_FIRST_DATA)) {
		throw new FlumeException("Invalid number of payload splits " + payloadSplits.length);
	    }

	    if (((payloadSplits.length - LDMS_INDEX_FIRST_DATA) % LDMS_PROCNETDEV_COLUMNS.length) != 0) {
		throw new FlumeException("Invalid number of payload splits " + payloadSplits.length);
	    }

	    // This will not work for each cluster, don't know interface

	    int numdevices = (payloadSplits.length - LDMS_INDEX_FIRST_DATA) / LDMS_PROCNETDEV_COLUMNS.length;

	    byte[] rowKey = calcRowkey(calcHostname(payloadSplits[LDMS_INDEX_HOSTNAME]),
				       calcTimestamp(payloadSplits[LDMS_INDEX_TIME]));

	    for (int i = 0; i < numdevices; i++) {
		byte[] devicenum = Integer.toString(numdevices).getBytes(Charsets.UTF_8);

		for (int j = 0; j < LDMS_PROCNETDEV_COLUMNS.length; j++) {
		    Put put = new Put(rowKey);
		    
		    byte[] val = payloadSplits[LDMS_INDEX_FIRST_DATA + i*LDMS_PROCNETDEV_COLUMNS.length + j].getBytes(Charsets.UTF_8);
		    // This is probably really slow, any way to do this faster?
		    // could calculate max field length and create buffer earlier, deal w/ later
		    byte[] col = new byte[LDMS_PROCNETDEV_COLUMNS[j].length + devicenum.length];
		    System.arraycopy(LDMS_PROCNETDEV_COLUMNS[j], 0, col, 0, LDMS_PROCNETDEV_COLUMNS[j].length);
		    System.arraycopy(devicenum, 0, col, LDMS_PROCNETDEV_COLUMNS[j].length, devicenum.length);
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
