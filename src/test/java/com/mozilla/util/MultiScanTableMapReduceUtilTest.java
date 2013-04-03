/**
 * Copyright 2012 Mozilla Foundation
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
package com.mozilla.util;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.junit.Before;
import org.junit.Test;

import com.mozilla.hadoop.hbase.mapreduce.MultiScanTableMapReduceUtil;

public class MultiScanTableMapReduceUtilTest {

    String dateFormat = "yyyyMMdd";
    Calendar start = Calendar.getInstance();
    Calendar end = Calendar.getInstance();
    List<Pair<String,String>> columns = new ArrayList<Pair<String,String>>(1);

    @Before
    public void setup() {
        columns.add(new Pair<String,String>("data", "json"));
    }

    @Test
    public void testMultiScanBytePrefix() {
        Scan[] scans = MultiScanTableMapReduceUtil.generateBytePrefixScans(start, end, dateFormat, columns, 0, false);

        // We should include scans for every byte from 0x00 to 0xFF
        assertEquals(256, scans.length);
    }

    @Test
    public void testMultiScanHexPrefix() {
        Scan[] scans = MultiScanTableMapReduceUtil.generateHexPrefixScans(start, end, dateFormat, columns, 0, false);

        // We should include scans for every hex char from '0' to 'f'
        assertEquals(16, scans.length);
    }
}
