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

public class LdmsVmstatHbaseEventSerializer extends LdmsHbaseEventSerializer {
    /*
      LDMS Vmstat Fields

      #Time, Time_usec, CompId, thp_split, thp_collapse_alloc_failed, thp_collapse_alloc, thp_fault_fallba ck, thp_fault_alloc, unevictable_pgs_mlockfreed, unevictable_pgs_stranded, unevictable_pgs_cleared, unevictable_pgs_munlocked, unevictable_pgs_mlocked, unevictable_pgs_rescued, unevictable_pgs_scanned , unevictable_pgs_culled, htlb_buddy_alloc_fail, htlb_buddy_alloc_success, compact_success, compact_ fail, compact_stall, compact_pagemigrate_failed, compact_pages_moved, compact_blocks_moved, pgrotate d, allocstall, pageoutrun, kswapd_skip_congestion_wait, kswapd_high_wmark_hit_quickly, kswapd_low_wm ark_hit_quickly, kswapd_inodesteal, kswapd_steal, slabs_scanned, pginodesteal, zone_reclaim_failed, pgscan_direct_movable, pgscan_direct_normal, pgscan_direct_dma32, pgscan_direct_dma, pgscan_kswapd_m ovable, pgscan_kswapd_normal, pgscan_kswapd_dma32, pgscan_kswapd_dma, pgsteal_movable, pgsteal_norma l, pgsteal_dma32, pgsteal_dma, pgrefill_movable, pgrefill_normal, pgrefill_dma32, pgrefill_dma, pgmajfault, pgfault, pgdeactivate, pgactivate, pgfree, pgalloc_movable, pgalloc_normal, pgalloc_dma32, p galloc_dma, pswpout, pswpin, pgpgout, pgpgin, nr_anon_transparent_hugepages, numa_other, numa_local, numa_interleave, numa_foreign, numa_miss, numa_hit, nr_shmem, nr_isolated_file, nr_isolated_anon, n r_writeback_temp, nr_vmscan_write, nr_bounce, nr_unstable, nr_kernel_stack, nr_page_table_pages, nr_ slab_unreclaimable, nr_slab_reclaimable, nr_writeback, nr_dirty, nr_file_pages, nr_mapped, nr_anon_pages, nr_mlock, nr_unevictable, nr_active_file, nr_inactive_file, nr_active_anon, nr_inactive_anon, nr_free_pages

    */

    final static private int LDMS_VMSTAT_LENGTH = 94;

    // Only for columsn we're going to store, we don't use some
    // Sorta ugly, but avoid constant byte conversions later

    final static private byte[][] LDMS_VMSTAT_COLUMNS = {
	"thp_split".getBytes(Charsets.UTF_8),
	"thp_collapse_alloc_failed".getBytes(Charsets.UTF_8),
	"thp_collapse_alloc".getBytes(Charsets.UTF_8),
	"thp_fault_fallba ck".getBytes(Charsets.UTF_8),
	"thp_fault_alloc".getBytes(Charsets.UTF_8),
	"unevictable_pgs_mlockfreed".getBytes(Charsets.UTF_8),
	"unevictable_pgs_stranded".getBytes(Charsets.UTF_8),
	"unevictable_pgs_cleared".getBytes(Charsets.UTF_8),
	"unevictable_pgs_munlocked".getBytes(Charsets.UTF_8),
	"unevictable_pgs_mlocked".getBytes(Charsets.UTF_8),
	"unevictable_pgs_rescued".getBytes(Charsets.UTF_8),
	"unevictable_pgs_scanned ".getBytes(Charsets.UTF_8),
	"unevictable_pgs_culled".getBytes(Charsets.UTF_8),
	"htlb_buddy_alloc_fail".getBytes(Charsets.UTF_8),
	"htlb_buddy_alloc_success".getBytes(Charsets.UTF_8),
	"compact_success".getBytes(Charsets.UTF_8),
	"compact_ fail".getBytes(Charsets.UTF_8),
	"compact_stall".getBytes(Charsets.UTF_8),
	"compact_pagemigrate_failed".getBytes(Charsets.UTF_8),
	"compact_pages_moved".getBytes(Charsets.UTF_8),
	"compact_blocks_moved".getBytes(Charsets.UTF_8),
	"pgrotate d".getBytes(Charsets.UTF_8),
	"allocstall".getBytes(Charsets.UTF_8),
	"pageoutrun".getBytes(Charsets.UTF_8),
	"kswapd_skip_congestion_wait".getBytes(Charsets.UTF_8),
	"kswapd_high_wmark_hit_quickly".getBytes(Charsets.UTF_8),
	"kswapd_low_wm ark_hit_quickly".getBytes(Charsets.UTF_8),
	"kswapd_inodesteal".getBytes(Charsets.UTF_8),
	"kswapd_steal".getBytes(Charsets.UTF_8),
	"slabs_scanned".getBytes(Charsets.UTF_8),
	"pginodesteal".getBytes(Charsets.UTF_8),
	"zone_reclaim_failed".getBytes(Charsets.UTF_8),
	"pgscan_direct_movable".getBytes(Charsets.UTF_8),
	"pgscan_direct_normal".getBytes(Charsets.UTF_8),
	"pgscan_direct_dma32".getBytes(Charsets.UTF_8),
	"pgscan_direct_dma".getBytes(Charsets.UTF_8),
	"pgscan_kswapd_m ovable".getBytes(Charsets.UTF_8),
	"pgscan_kswapd_normal".getBytes(Charsets.UTF_8),
	"pgscan_kswapd_dma32".getBytes(Charsets.UTF_8),
	"pgscan_kswapd_dma".getBytes(Charsets.UTF_8),
	"pgsteal_movable".getBytes(Charsets.UTF_8),
	"pgsteal_norma l".getBytes(Charsets.UTF_8),
	"pgsteal_dma32".getBytes(Charsets.UTF_8),
	"pgsteal_dma".getBytes(Charsets.UTF_8),
	"pgrefill_movable".getBytes(Charsets.UTF_8),
	"pgrefill_normal".getBytes(Charsets.UTF_8),
	"pgrefill_dma32".getBytes(Charsets.UTF_8),
	"pgrefill_dma".getBytes(Charsets.UTF_8),
	"pgmajfault".getBytes(Charsets.UTF_8),
	"pgfault".getBytes(Charsets.UTF_8),
	"pgdeactivate".getBytes(Charsets.UTF_8),
	"pgactivate".getBytes(Charsets.UTF_8),
	"pgfree".getBytes(Charsets.UTF_8),
	"pgalloc_movable".getBytes(Charsets.UTF_8),
	"pgalloc_normal".getBytes(Charsets.UTF_8),
	"pgalloc_dma32".getBytes(Charsets.UTF_8),
	"p galloc_dma".getBytes(Charsets.UTF_8),
	"pswpout".getBytes(Charsets.UTF_8),
	"pswpin".getBytes(Charsets.UTF_8),
	"pgpgout".getBytes(Charsets.UTF_8),
	"pgpgin".getBytes(Charsets.UTF_8),
	"nr_anon_transparent_hugepages".getBytes(Charsets.UTF_8),
	"numa_other".getBytes(Charsets.UTF_8),
	"numa_local".getBytes(Charsets.UTF_8),
	"numa_interleave".getBytes(Charsets.UTF_8),
	"numa_foreign".getBytes(Charsets.UTF_8),
	"numa_miss".getBytes(Charsets.UTF_8),
	"numa_hit".getBytes(Charsets.UTF_8),
	"nr_shmem".getBytes(Charsets.UTF_8),
	"nr_isolated_file".getBytes(Charsets.UTF_8),
	"nr_isolated_anon".getBytes(Charsets.UTF_8),
	"n r_writeback_temp".getBytes(Charsets.UTF_8),
	"nr_vmscan_write".getBytes(Charsets.UTF_8),
	"nr_bounce".getBytes(Charsets.UTF_8),
	"nr_unstable".getBytes(Charsets.UTF_8),
	"nr_kernel_stack".getBytes(Charsets.UTF_8),
	"nr_page_table_pages".getBytes(Charsets.UTF_8),
	"nr_ slab_unreclaimable".getBytes(Charsets.UTF_8),
	"nr_slab_reclaimable".getBytes(Charsets.UTF_8),
	"nr_writeback".getBytes(Charsets.UTF_8),
	"nr_dirty".getBytes(Charsets.UTF_8),
	"nr_file_pages".getBytes(Charsets.UTF_8),
	"nr_mapped".getBytes(Charsets.UTF_8),
	"nr_anon_pages".getBytes(Charsets.UTF_8),
	"nr_mlock".getBytes(Charsets.UTF_8),
	"nr_unevictable".getBytes(Charsets.UTF_8),
	"nr_active_file".getBytes(Charsets.UTF_8),
	"nr_inactive_file".getBytes(Charsets.UTF_8),
	"nr_active_anon".getBytes(Charsets.UTF_8),
	"nr_inactive_anon".getBytes(Charsets.UTF_8),
	"nr_free_pages".getBytes(Charsets.UTF_8),
    };

    public LdmsVmstatHbaseEventSerializer(){

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

	    if (payloadSplits.length != LDMS_VMSTAT_LENGTH) {
		throw new FlumeException("Invalid number of payload splits " + payloadSplits.length);
	    }

	    byte[] rowKey = calcRowkey(calcHostname(payloadSplits[LDMS_INDEX_HOSTNAME]),
				       calcTimestamp(payloadSplits[LDMS_INDEX_TIME]));

	    for (int i = 0; i < LDMS_VMSTAT_COLUMNS.length; i++) {
		Put put = new Put(rowKey);

		byte[] val = payloadSplits[LDMS_INDEX_FIRST_DATA + i].getBytes(Charsets.UTF_8);
		put.add(this.columnFamily, LDMS_VMSTAT_COLUMNS[i], val);
		actions.add(put);
	    }
	} catch (Exception e) {
	    throw new FlumeException("Could not put in row!", e);
	}
	    
	return actions;
    }
}
