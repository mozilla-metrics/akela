/**
 * Copyright 2011 Mozilla Foundation
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

package com.mozilla.pig.filter;

import java.io.IOException;
import java.util.Properties;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;

public class IsBlacklistIP extends FilterFunc {
	
	private String[] blacklist = null;

	public IsBlacklistIP() {
		Properties props = UDFContext.getUDFContext().getClientSystemProps();
		if (props.containsKey("ip.addr.blacklist")) {
			blacklist = props.getProperty("ip.addr.blacklist").split(",");
		}
		
		if (blacklist == null) {
			throw new RuntimeException("ip.addr.blacklist was not specified");
		}
	}
	
	/* (non-Javadoc)
	 * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
	 */
	public Boolean exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return null;
		}

		String currentIp = (String)input.get(0);
		for (String ip : blacklist) {
			if (ip.equals((currentIp))) {
				return true;
			}
		}

		return false;
	}
	
}
