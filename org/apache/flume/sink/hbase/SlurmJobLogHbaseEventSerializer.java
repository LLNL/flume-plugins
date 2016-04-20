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

import java.io.IOException;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;

import com.google.common.base.Charsets;

public class SlurmJobLogHbaseEventSerializer implements HbaseEventSerializer {
    final static String clusterNameKey = "clustername";

    protected String clusterName;

    protected byte[] columnFamily;
    protected byte[] payload;

    // Sorta ugly, but want to loop and want to avoid constant byte conversions later

    final static private String[] SLURMJOBLOG_KEYS = {
	"JobId",
	"UserId",
	"Name",
	"JobState",
	"Partition",
	"TimeLimit",
	"StartTime",
	"EndTime",
	"NodeList",
	"NodeCnt",
	"Procs",
    };

    final static private String SLURMJOBLOG_JOBID = SLURMJOBLOG_KEYS[0];

    final static private byte[][] SLURMJOBLOG_KEYS_BYTES = {
	SLURMJOBLOG_KEYS[0].getBytes(Charsets.UTF_8),
	SLURMJOBLOG_KEYS[1].getBytes(Charsets.UTF_8),
	SLURMJOBLOG_KEYS[2].getBytes(Charsets.UTF_8),
	SLURMJOBLOG_KEYS[3].getBytes(Charsets.UTF_8),
	SLURMJOBLOG_KEYS[4].getBytes(Charsets.UTF_8),
	SLURMJOBLOG_KEYS[5].getBytes(Charsets.UTF_8),
	SLURMJOBLOG_KEYS[6].getBytes(Charsets.UTF_8),
	SLURMJOBLOG_KEYS[7].getBytes(Charsets.UTF_8),
	SLURMJOBLOG_KEYS[8].getBytes(Charsets.UTF_8),
	SLURMJOBLOG_KEYS[9].getBytes(Charsets.UTF_8),
	SLURMJOBLOG_KEYS[10].getBytes(Charsets.UTF_8),
    };

    public SlurmJobLogHbaseEventSerializer(){

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

        clusterName = headers.get(clusterNameKey);
    }

    /* From: http://stackoverflow.com/questions/14768171/convert-string-representing-key-value-pairs-to-map */
    private static Map<String, String> keyvalueparse(String str) {
	String[] tokens = str.split(" |=");
	Map<String, String> map = new HashMap<String, String>();
	for (int i=0; i<tokens.length-1; )
	    map.put(tokens[i++], tokens[i++]);
	return map;
    }

    @Override
    public List<Row> getActions() throws FlumeException {
        List<Row> actions = new LinkedList<Row>();

        try {
            String payloadStr = new String(this.payload, "UTF-8");

	    /* Expected format
	     * 
	     * JobId=1841909 UserId=achu(4299) Name=myjob JobState=COMPLETED Partition=pbatch TimeLimit=1320 StartTime=2016-03-12T15:04:03 EndTime=2016-03-12T16:17:27 NodeList=mycluster[239-251,253-289] NodeCnt=50 Procs=1200
	     *
	     */
	    Map<String, String> kv = keyvalueparse(payloadStr);

	    // We are going to assume line valid if it has atleast the
	    // JobId.  User is required to check for valid input on
	    // everything else.

	    if (!kv.containsKey(SLURMJOBLOG_JOBID) || kv.get(SLURMJOBLOG_JOBID) == null) {
		throw new FlumeException("Could not put in row - line missing JobId" );
	    }

            byte[] rowKey = ("slurmjoblog-" + this.clusterName + "-" + kv.get("JobId")).getBytes("UTF8");

            for (int i = 0; i < SLURMJOBLOG_KEYS.length; i++) {
		if (kv.containsKey(SLURMJOBLOG_KEYS[i])) {
		    Put put = new Put(rowKey);

		    String value = kv.get(SLURMJOBLOG_KEYS[i]); 
		    byte[] val = value.getBytes(Charsets.UTF_8);
		    put.add(this.columnFamily, SLURMJOBLOG_KEYS_BYTES[i], val);
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
