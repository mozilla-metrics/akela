/* ***** BEGIN LICENSE BLOCK *****
 * Version: MPL 1.1/GPL 2.0/LGPL 2.1
 *
 * The contents of this file are subject to the Mozilla Public License Version
 * 1.1 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS IS" basis,
 * WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 * for the specific language governing rights and limitations under the
 * License.
 *
 * The Original Code is Mozilla Akela.
 *
 * The Initial Developer of the Original Code is the Mozilla Foundation.
 * Portions created by the Initial Developer are Copyright (C) 2010
 * the Initial Developer. All Rights Reserved.
 *
 * Contributor(s):
 * 
 *   Xavier Stevens <xstevens@mozilla.com>, Mozilla Corporation (original author)
 *
 * Alternatively, the contents of this file may be used under the terms of
 * either the GNU General Public License Version 2 or later (the "GPL"), or
 * the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
 * in which case the provisions of the GPL or the LGPL are applicable instead
 * of those above. If you wish to allow use of your version of this file only
 * under the terms of either the GPL or the LGPL, and not to allow others to
 * use your version of this file under the terms of the MPL, indicate your
 * decision by deleting the provisions above and replace them with the notice
 * and other provisions required by the GPL or the LGPL. If you do not delete
 * the provisions above, a recipient may use your version of this file under
 * the terms of any one of the MPL, the GPL or the LGPL.
 *
 * ***** END LICENSE BLOCK ***** */
 
package com.mozilla.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.IOError;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;


public class ClusterHealth {

	private static final byte[] META_TABLE_NAME = Bytes.toBytes(".META.");
	
	private static boolean testThrift(String host) {
		boolean ret = false;
        TTransport transport = null;
        try {
            transport = new TSocket(host, 9090, 3000);
            Hbase.Client client = new Hbase.Client(new TBinaryProtocol(transport));
            transport.open();
            client.getColumnDescriptors(META_TABLE_NAME);
            System.out.println(String.format("%s ThriftServer - [ ALIVE ]", new Object[] { host }));
            ret = true;
        } catch (TTransportException e) {
        	System.out.println(String.format("%s ThriftServer - [ DEAD ] - %s", new Object[] { host, e.getMessage() }));
		} catch (IOError e) {
        	System.out.println(String.format("%s ThriftServer - [ DEAD ] - %s", new Object[] { host, e.getMessage() }));
		} catch (TException e) {
        	System.out.println(String.format("%s ThriftServer - [ DEAD ] - %s", new Object[] { host, e.getMessage() }));
		} finally {
        	if (transport != null) {
        		transport.close();
        	}
        }
		
        return ret;
    }
	
	public static void main(String[] args) {
		int retCode = 0;
		
		Configuration conf = new Configuration();
		System.out.println("HDFS NameNode: " + conf.get("fs.default.name"));
		DFSClient dfsClient = null;
		try {
			dfsClient = new DFSClient(conf);
			
			DatanodeInfo[] liveNodes = dfsClient.datanodeReport(DatanodeReportType.LIVE);
			for (DatanodeInfo dni : liveNodes) {
				long dfsUsed = dni.getDfsUsed();
				long nonDfsUsed = dni.getNonDfsUsed();
				long capacity = dni.getCapacity();
				float capacityPercentage = ((float)(dfsUsed + nonDfsUsed) / (float)capacity) * 100.0f;
				System.out.println(String.format("%s DataNode - [ ALIVE ] - DFS Capacity: (%d + %d / %d) %.2f%%; xceivers: %d", 
	  					 new Object[] { dni.getHostName(), dfsUsed, nonDfsUsed, capacity, capacityPercentage, dni.getXceiverCount() }));
			}
			DatanodeInfo[] deadNodes = dfsClient.datanodeReport(DatanodeReportType.DEAD);
			if (deadNodes.length > 0) {
				retCode = 2;
				for (DatanodeInfo dni : deadNodes) {
					System.out.println(dni.getHostName() + " DataNode - [ DEAD ]");
				}
			}
		} catch (IOException e) {
			retCode = 2;
			System.out.println("IOException occurred while checking HDFS cluster status!");
			e.printStackTrace(System.err);
		} finally {
			if (dfsClient != null) {
				try {
					dfsClient.close();
				} catch (IOException e) {
					System.out.println("IOException occurred while closing DFS client!");
					e.printStackTrace(System.err);
				}
			}
		}
		
		Configuration hbaseConf = HBaseConfiguration.create(conf);
		HBaseAdmin hbaseAdmin;
		try {
			System.out.println("HBase Rootdir: " + hbaseConf.get("hbase.rootdir"));
			hbaseAdmin = new HBaseAdmin(hbaseConf);
			ClusterStatus hcs = hbaseAdmin.getClusterStatus();
			int regionsCount = hcs.getRegionsCount();
			int requestsCount = hcs.getRequestsCount();
			for (HServerInfo serverInfo : hcs.getServerInfo()) {
				HServerLoad hsl = serverInfo.getLoad();
				float heapPercentage = ((float)hsl.getUsedHeapMB() / (float)hsl.getMaxHeapMB()) * 100.0f;
				float regionsPercentage = regionsCount == 0 ? 0.0f : ((float)hsl.getNumberOfRegions() / (float)regionsCount) * 100.0f;
				float requestsPercentage = requestsCount == 0 ? 0.0f : ((float)hsl.getNumberOfRequests() / (float)requestsCount) * 100.0f;
				System.out.println(String.format("%s RegionServer - [ ALIVE ] - Memory Heap: (%d / %d MB) %.2f%%, Regions: (%d / %d) %.2f%%, Requests: (%d / %d) %.2f%%", 
							  					 new Object[] { serverInfo.getHostname(), hsl.getUsedHeapMB(), hsl.getMaxHeapMB(), 
											 	 heapPercentage, hsl.getNumberOfRegions(), regionsCount, regionsPercentage, hsl.getNumberOfRequests(), requestsCount, requestsPercentage }));
			}
			if (hcs.getDeadServers() > 0) {
				retCode = 2;
				for (String server : hcs.getDeadServerNames()) {
					System.out.println(server + " RegionServer - [ DEAD ]");
				}
			}
			
		} catch (MasterNotRunningException e) {
			System.out.println("HBase Master is not running!");
			retCode = 2;
		} catch (IOException e) {
			System.out.println("IOException occurred while checking HBase cluster status!");
			retCode = 2;
		}
		
		int failures = 0;
        for (String host : args) {
            if (!ClusterHealth.testThrift(host)) {
                failures++;
            }
        }
        if (failures > 0) {
        	retCode = 2;
        }
        
		System.exit(retCode);
	}
	
}
