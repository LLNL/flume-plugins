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

public class LdmsHbaseEventSerializer implements HbaseEventSerializer {
    final static String clusterNameKey = "clustername";
    final static String sourceTypeKey = "sourcetype";

    protected String clusterName;
    protected String sourceType;

    protected byte[] columnFamily;
    protected byte[] payload;

    final static protected int LDMS_INDEX_TIME = 0;
    final static protected int LDMS_INDEX_TIMEUSEC = 1;
    final static protected int LDMS_INDEX_HOSTNAME = 2;
    final static protected int LDMS_INDEX_FIRST_DATA = 3;

    public LdmsHbaseEventSerializer(){

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


    // Basic implementation assuming the most trivial commit
    @Override
    public List<Row> getActions() throws FlumeException {
	List<Row> actions = new LinkedList<Row>();

	if (payloadValid() == false) {
	    return actions;
	}

	try {
	    byte[] rowKey = calcRowkey();

	    Put put = new Put(rowKey);

	    byte[] col = "LDMSPayload".getBytes(Charsets.UTF_8);
	    put.add(this.columnFamily, col, payload);
	    actions.add(put);
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

    protected boolean payloadValid() {
	// Small chance line is empty
	if (this.payload.length == 0) {
	    return false;
	}

	// Small chance this is the header
	if (this.payload[0] == '#') {
	    return false;
	}

	return true;
    }

    protected String calcHostname(String hostNameInt) {
	return (clusterName + String.format("%05d", Integer.parseInt(hostNameInt)));
    }

    protected String calcTimestamp(String timestamp) throws IOException {
	String[] ts = timestamp.split("\\.");
	return (ts[0]);
    }

    protected byte[] calcRowkey(String hostName, String time) throws IOException {
	return ("ldms-" + this.sourceType + "-" + this.clusterName + "-" + time + "-" + hostName).getBytes("UTF8");
    }

    protected byte[] calcRowkey(String time) throws IOException {
	return ("ldms-" + this.sourceType + "-" + this.clusterName + "-" + time).getBytes("UTF8");
    }

    protected byte[] calcRowkey() throws IOException {
	return ("ldms-" + this.sourceType + "-" + this.clusterName + "-" + String.valueOf(System.currentTimeMillis())).getBytes("UTF8");
    }
}
