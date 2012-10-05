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
package com.mozilla.pig.load;

import java.util.regex.Pattern;

/**
 * CustomRegExLoader lets you specify an arbitrary Regular Expression to use parsing lines.
 */
public class CustomRegExLoader extends RegExLoader {

    private final Pattern customPattern;

    // Default to matching entire line.
    public CustomRegExLoader() {
        this.customPattern = Pattern.compile("^(.*)$");
    }

    public CustomRegExLoader(String aPattern) {
        this.customPattern = Pattern.compile(aPattern);
    }

    @Override
    public Pattern getPattern() {
        return customPattern;
    }

}
