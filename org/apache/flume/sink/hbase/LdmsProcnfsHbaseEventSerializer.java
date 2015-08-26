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

public class LdmsProcnfsHbaseEventSerializer extends LdmsHbaseEventSerializer {
    /*
      LDMS Procnfs Fields

      #Time, Time_usec, CompId, commit, pathconf, fsinfo, fsstat,
       readdirplus, readdir, link, rename, rmdir, remove, mknod,
       symlink, mkdir, create, write, read, readlink, access, lookup,
       setattr, getattr, retransmitts, numcalls

    */

    final static private int LDMS_PROCNFS_LENGTH = 26;

    // Only for columsn we're going to store, we don't use some
    // Sorta ugly, but avoid constant byte conversions later

    final static private byte[][] LDMS_PROCNFS_COLUMNS = {
	"commit".getBytes(Charsets.UTF_8),
	"pathconf".getBytes(Charsets.UTF_8),
	"fsinfo".getBytes(Charsets.UTF_8),
	"fsstat".getBytes(Charsets.UTF_8),
	"readdirplus".getBytes(Charsets.UTF_8),
	"readdir".getBytes(Charsets.UTF_8),
	"link".getBytes(Charsets.UTF_8),
	"rename".getBytes(Charsets.UTF_8),
	"rmdir".getBytes(Charsets.UTF_8),
	"remove".getBytes(Charsets.UTF_8),
	"mknod".getBytes(Charsets.UTF_8),
	"symlink".getBytes(Charsets.UTF_8),
	"mkdir".getBytes(Charsets.UTF_8),
	"create".getBytes(Charsets.UTF_8),
	"write".getBytes(Charsets.UTF_8),
	"read".getBytes(Charsets.UTF_8),
	"readlink".getBytes(Charsets.UTF_8),
	"access".getBytes(Charsets.UTF_8),
	"lookup".getBytes(Charsets.UTF_8),
	"setattr".getBytes(Charsets.UTF_8),
	"getattr".getBytes(Charsets.UTF_8),
	"retransmitts".getBytes(Charsets.UTF_8),
	"numcalls".getBytes(Charsets.UTF_8),
    };

    public LdmsProcnfsHbaseEventSerializer(){

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

	    if (payloadSplits.length != LDMS_PROCNFS_LENGTH) {
		throw new FlumeException("Invalid number of payload splits " + payloadSplits.length);
	    }

	    byte[] rowKey = calcRowkey(calcHostname(payloadSplits[LDMS_INDEX_HOSTNAME]),
				       calcTimestamp(payloadSplits[LDMS_INDEX_TIME]));

	    for (int i = 0; i < LDMS_PROCNFS_COLUMNS.length; i++) {
		Put put = new Put(rowKey);

		byte[] val = payloadSplits[LDMS_INDEX_FIRST_DATA + i].getBytes(Charsets.UTF_8);
		put.add(this.columnFamily, LDMS_PROCNFS_COLUMNS[i], val);
		actions.add(put);
	    }
	} catch (Exception e) {
	    throw new FlumeException("Could not put in row!", e);
	}
	    
	return actions;
    }
}
