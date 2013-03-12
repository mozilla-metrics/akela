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

import static java.util.Calendar.DATE;
import static java.util.Calendar.HOUR_OF_DAY;
import static java.util.Calendar.MILLISECOND;
import static java.util.Calendar.MINUTE;
import static java.util.Calendar.SECOND;
import static java.util.Calendar.WEEK_OF_YEAR;

import java.util.Calendar;

public class DateUtil {

    public static long DAY_IN_MILLIS = 1000 * 60 * 60 * 24;
    public static long WEEK_IN_MILLIS = DAY_IN_MILLIS * 7;
    
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
			case DATE:
				cal.set(HOUR_OF_DAY, 0);
			case HOUR_OF_DAY:
				cal.set(MINUTE, 0);
			case MINUTE:
				cal.set(SECOND, 0);
			case SECOND:
				cal.set(MILLISECOND, 0);
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
			case DATE:
				cal.set(HOUR_OF_DAY, 23);
			case HOUR_OF_DAY:
				cal.set(MINUTE, 59);
			case MINUTE:
				cal.set(SECOND, 59);
			case SECOND:
				cal.set(MILLISECOND, 999);
			default:
				break;
		}
		
		return cal.getTimeInMillis();
	}
	
	/**
	 * Get the time difference in the specified deltaUnit between the start and end time
	 * @param start
	 * @param end
	 * @param deltaUnit
	 * @return
	 */
	public static long getTimeDelta(long start, long end, int deltaUnit) {
	    long delta = 0;
	    switch (deltaUnit) {
	        case DATE:
                delta = Math.round((double)(end - start) / DAY_IN_MILLIS);
	            break;
	        case WEEK_OF_YEAR:
                delta = Math.round((double)(end - start) / WEEK_IN_MILLIS);
	            break;
	        case MILLISECOND:
	            // pass through to default
	        default:
	            delta = (end - start);
	            break;
	    }
	    
	    return delta;
	}
	
	/**
	 * Iterate from startMillis to endMillis (including endMillis) in increments of incrementCount * incrementType
	 * @param startMillis
	 * @param endMillis
	 * @param incrementType
	 * @param incrementCount
	 * @param iterator
	 */
	public static void iterateByTime(long startMillis, long endMillis, int incrementType, int incrementCount, DateIterator iterator) {
	    Calendar start = Calendar.getInstance();
        start.setTimeInMillis(startMillis);
        Calendar end = Calendar.getInstance();
        end.setTimeInMillis(endMillis);
        while (!start.after(end)) {
            iterator.see(start.getTimeInMillis());
            start.add(incrementType, incrementCount);
        }
	}
	
	/**
	 * Iterate from startMillis to endMillis (including endMillis) in increments of one day
	 * @param startMillis
	 * @param endMillis
	 * @param iterator
	 */
    public static void iterateByDay(long startMillis, long endMillis, DateIterator iterator) {
        iterateByTime(startMillis, endMillis, DATE, 1, iterator);
    }
}