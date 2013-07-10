/**
 * Copyright 2010 Mozilla Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package com.mozilla.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.IOError;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.nio.ByteBuffer;


public class ClusterHealth {

	private static final byte[] META_TABLE_NAME = Bytes.toBytes(".META.");
	
	private static boolean testThrift(String host) {
		boolean ret = false;
        TTransport transport = null;
        try {
            transport = new TSocket(host, 9090, 3000);
            Hbase.Client client = new Hbase.Client(new TBinaryProtocol(transport));
            transport.open();
            client.getColumnDescriptors(ByteBuffer.wrap(META_TABLE_NAME));
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
			for (ServerName server : hcs.getServers()) {
				HServerLoad hsl = hcs.getLoad(server);
				float heapPercentage = ((float)hsl.getUsedHeapMB() / (float)hsl.getMaxHeapMB()) * 100.0f;
				float regionsPercentage = regionsCount == 0 ? 0.0f : ((float)hsl.getNumberOfRegions() / (float)regionsCount) * 100.0f;
				float requestsPercentage = requestsCount == 0 ? 0.0f : ((float)hsl.getNumberOfRequests() / (float)requestsCount) * 100.0f;
				System.out.println(String.format("%s RegionServer - [ ALIVE ] - Memory Heap: (%d / %d MB) %.2f%%, Regions: (%d / %d) %.2f%%, Requests: (%d / %d) %.2f%%", 
							  					 new Object[] { server.getHostname(), hsl.getUsedHeapMB(), hsl.getMaxHeapMB(),
											 	 heapPercentage, hsl.getNumberOfRegions(), regionsCount, regionsPercentage, hsl.getNumberOfRequests(), requestsCount, requestsPercentage }));
			}
			if (hcs.getDeadServers() > 0) {
				retCode = 2;
				for (ServerName server : hcs.getDeadServerNames()) {
					System.out.println(server.getHostname() + " RegionServer - [ DEAD ]");
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
