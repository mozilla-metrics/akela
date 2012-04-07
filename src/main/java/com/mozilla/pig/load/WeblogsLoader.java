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
package com.mozilla.pig.load;

import java.util.regex.*;

import com.mozilla.pig.load.RegExLoader;

/**
 * WeblogsLoader is inspired by CombinedLogLoader/RegexLoader in piggybank. 
 * 
 * Example Line: 192.168.1.21
 * addons.mozilla.org - [16/Jan/2011:05:00:48 -0800] "GET /foo/bar/baz/ HTTP/1.1" 200 4840 "-"
 * "Mozilla/5.0 (Windows; U; Windows NT 5.1; ko; rv:1.9.2.3) Gecko/20100401 Firefox/3.6.13" "-"
 */
public class WeblogsLoader extends RegExLoader {

    private static final String GENERIC_FIELD = "([^\\s]*)";
    private static final String GENERIC_REQUIRED_FIELD = "([^\\s]+)";
    private static final String DIGIT_FIELD = "(\\d+)";
    private static final String QUOTED_FIELD = "\"?(.*)\"?";
    private static final String SPACER = "\\s";

    private static final String IP_ADDRESS = "([\\d.]{7,15})";
    private static final String DATE_TIME = "\\[(\\d{2}/\\w{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}\\s[-+]\\d{4})\\]";
    private static final String METHOD_URI_PROTOCOL = "\"([A-Z]*)\\s([^\\s]*)\\s([^\\s]*)\"";

    private final Pattern webLogPattern;

    public WeblogsLoader() {
        this.webLogPattern = Pattern.compile(buildPattern(false));
    }

    public WeblogsLoader(String isDNTLogs) {
        this.webLogPattern = Pattern.compile(buildPattern(Boolean.parseBoolean(isDNTLogs)));
    }

    /**
     * Build the pattern using predefined components so its easier to read
     * 
     * @return
     */
    /*
     * "^(?>([\\d.]{7,15})\\s([^\\s]*)\\s([^\\s]*)\\s\\[(\\d{2}/\\w{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}\\s[-+]\\d{4})\\]\\s)(?>\"([A-Z]+)\\s([^\\s]*)\\sHTTP/1\\.[01]\"\\s(\\d{3})\\s(\\d+)\\s\"([^\"]+)\"\\s)(?>\"\"?([^\"]*)\"?\")(?>\\s\"([^\"]*)\")?$"
     */
    private String buildPattern(boolean isDNTLogs) {
        StringBuilder pb = new StringBuilder("^");
        pb.append(IP_ADDRESS).append(SPACER); // ip_address
        pb.append(GENERIC_FIELD).append(SPACER); // log_name
        pb.append(GENERIC_FIELD).append(SPACER); // empty
        pb.append(DATE_TIME).append(SPACER); // date and time
        pb.append(METHOD_URI_PROTOCOL).append(SPACER); // method, uri, http protocol
        pb.append(GENERIC_REQUIRED_FIELD).append(SPACER); // http return code
        pb.append(DIGIT_FIELD).append(SPACER); // bytes
        pb.append(QUOTED_FIELD).append(SPACER); // referrer
        pb.append(QUOTED_FIELD).append(SPACER); // user_agent
        pb.append(QUOTED_FIELD); // cookie
        if (isDNTLogs) {
            pb.append(SPACER).append(QUOTED_FIELD); // DNT
        }
        pb.append("?$");

        return pb.toString();
    }

    public Pattern getPattern() {
        return webLogPattern;
    }

}
