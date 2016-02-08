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

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;

import com.google.common.base.Charsets;

public class CSVGenericHbaseEventSerializer implements HbaseEventSerializer {
    final static String clusterNameKey = "clustername";
    final static String sourceTypeKey = "sourcetype";
    final static String csvheaderKey = "csvheader";

    protected String clusterName;
    protected String sourceType;
    protected String csvheader;

    protected byte[] columnFamily;
    protected byte[] payload;

    protected String[] csvheaderfields;
    protected byte[][] csvheaderfieldsBytes;

    static int myinc = 0;

    public CSVGenericHbaseEventSerializer(){

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

        if (!headers.containsKey(csvheaderKey)) {
            throw new FlumeException("Event does not contain header '" + csvheaderKey + "'");
        }

        clusterName = headers.get(clusterNameKey);
        sourceType = headers.get(sourceTypeKey);
        csvheader = headers.get(csvheaderKey);

        // Remove '#' if necessary
        if (csvheader.charAt(0) == '#') {
            csvheader = csvheader.substring(1, csvheader.length());
            csvheader = csvheader.trim();
        }

        csvheaderfields = csvheader.split("\\s*[, ]\\s*");

        if (csvheaderfields.length == 0) {
            throw new FlumeException("Event csvheader is empty");
        }

        csvheaderfieldsBytes = new byte[csvheaderfields.length][];

        for (int i = 0; i < csvheaderfields.length; i++) {
            csvheaderfieldsBytes[i] = csvheaderfields[i].getBytes(Charsets.UTF_8);
        } 
    }


    // Basic implementation assuming the most trivial commit
    @Override
    public List<Row> getActions() throws FlumeException {
        List<Row> actions = new LinkedList<Row>();

        // Small chance this is the header
        if (payload[0] == '#') {
            return actions;
        }

        try {
            String payloadStr = new String(this.payload, "UTF-8");
            String[] payloadSplits = payloadStr.split("\\s*[, ]\\s*");

            // csvheader check
            if (payloadSplits.length != csvheaderfields.length) {
                throw new FlumeException("Number of fields in event = " + payloadSplits.length + " != fields in csv header = " + csvheaderfields.length);
            }

            byte[] rowKey = (this.sourceType + "-" + this.clusterName + "-" + String.valueOf(System.currentTimeMillis()) + "-" + myinc).getBytes("UTF8");
            myinc++;

            for (int i = 0; i < payloadSplits.length; i++) {
                Put put = new Put(rowKey);

                byte[] val = payloadSplits[i].getBytes(Charsets.UTF_8);
                put.add(this.columnFamily, csvheaderfieldsBytes[i], val);
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
