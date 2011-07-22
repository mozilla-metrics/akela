/*
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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mozilla.util;

public class Pair<X extends Comparable<X>, Y> implements Comparable<Pair<X,Y>> {
	
	public X first;
	public Y second;
	
	/**
	 * Default Constructor
	 */
	public Pair() {
	}
	
	/**
	 * @param first
	 * @param second
	 */
	public Pair(X first, Y second) {
		this.first = first;
		this.second = second;
	}

	/**
	 * @return Returns the first.
	 */
	public X getFirst() {
		return first;
	}

	/**
	 * @param first The first to set.
	 */
	public void setFirst(X first) {
		this.first = first;
	}

	/**
	 * @return Returns the second.
	 */
	public Y getSecond() {
		return second;
	}

	/**
	 * @param second The second to set.
	 */
	public void setSecond(Y second) {
		this.second = second;
	}

	public int compareTo(Pair<X,Y> o) {
	    final int EQUAL = 0;

	    //this optimization is usually worthwhile, and can
	    //always be added
	    if ( this == o ) return EQUAL;

	    //objects, including type-safe enums, follow this form
	    //note that null objects will throw an exception here
	    int comparison = first.compareTo(o.getFirst());
	    if ( comparison != EQUAL ) return comparison;
	    
	    //all comparisons have yielded equality
	    //verify that compareTo is consistent with equals (optional)
	    assert this.equals(o) : "compareTo inconsistent with equals.";

	    return EQUAL;
	}
	
	@SuppressWarnings("unchecked")
	public boolean equals(Object o) {
		Pair<X,Y> otherPair = (Pair<X,Y>)o;
		return this.getFirst().equals(otherPair.getFirst());
	}
	
	public String toString() {
		return "(" + this.getFirst() + ", " + this.getSecond() + ")";
	}
}
