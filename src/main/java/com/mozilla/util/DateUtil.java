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

package com.mozilla.util;

import java.util.Calendar;

public class DateUtil {

	/**
	 * Get the first moment in time for the given time and resolution
	 * @param time
	 * @param resolution
	 * @return
	 */
	public static long getTimeAtResolution(long time, int resolution) {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(time);
		
		switch (resolution) {
			case Calendar.DATE:
				cal.set(Calendar.HOUR, 0);
			case Calendar.HOUR:
				cal.set(Calendar.MINUTE, 0);
			case Calendar.MINUTE:
				cal.set(Calendar.SECOND, 0);
			case Calendar.SECOND:
				cal.set(Calendar.MILLISECOND, 0);
			default:
				break;
		}
		
		return cal.getTimeInMillis();
	}

	/**
	 * Get the last moment in time for the given time and resolution
	 * @param time
	 * @param resolution
	 * @return
	 */
	public static long getEndTimeAtResolution(long time, int resolution) {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(time);
		
		switch (resolution) {
			case Calendar.DATE:
				cal.set(Calendar.HOUR, 23);
			case Calendar.HOUR:
				cal.set(Calendar.MINUTE, 59);
			case Calendar.MINUTE:
				cal.set(Calendar.SECOND, 59);
			case Calendar.SECOND:
				cal.set(Calendar.MILLISECOND, 999);
			default:
				break;
		}
		
		return cal.getTimeInMillis();
	}
	
}
