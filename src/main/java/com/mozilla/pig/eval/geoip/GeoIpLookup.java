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
package com.mozilla.pig.eval.geoip;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;

public class GeoIpLookup extends EvalFunc<Tuple> {

    private static final String EMPTY_STRING = "";
    
    private LookupService lookupService;
    private TupleFactory tupleFactory = TupleFactory.getInstance();
    
    public GeoIpLookup(String filename) throws IOException {
        lookupService = new LookupService(filename);
    }

    @Override
    public Tuple exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        
        String ip = (String)input.get(0);
        Location location = lookupService.getLocation(ip);
        if (location != null) {
            Tuple output = tupleFactory.newTuple(6);
            output.set(0, location.countryName != null ? location.countryName : EMPTY_STRING);
            output.set(1, location.countryCode != null ? location.countryCode : EMPTY_STRING);
            output.set(2, location.region != null ? location.region : EMPTY_STRING);
            output.set(3, location.city != null ? location.city : EMPTY_STRING);
            output.set(4, location.postalCode != null ? location.postalCode : EMPTY_STRING);
            output.set(5, location.metro_code);
            
            return output;
        }
        
        return null;
    }
    
}
