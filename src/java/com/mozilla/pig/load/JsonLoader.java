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
 
package com.mozilla.pig.load;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigFileInputFormat;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonLoader extends LoadFunc {
	
	private static final Logger LOG = LoggerFactory.getLogger(JsonLoader.class);
	private static final TupleFactory tupleFactory_ = TupleFactory.getInstance();
	
	private ObjectMapper jsonMapper = new ObjectMapper();
	private LineRecordReader in = null;
	
	public JsonLoader() {
	}

	@SuppressWarnings("unchecked")
	@Override
	public InputFormat getInputFormat() throws IOException {
		return new PigTextInputFormat();
	}

	@Override
	public Tuple getNext() throws IOException {
		boolean notDone = in.nextKeyValue();
		if (!notDone) {
			return null;
		}

		String line;
		Text val = in.getCurrentValue();
		if (val == null) {
			return null;
		}

		line = val.toString();
		if (line.length() > 0) {
			Tuple t = parseStringToTuple(line);

			if (t != null) {
				return t;
			}
		}

		return null;
	}

	protected Tuple parseStringToTuple(String line) {
		try {
			Map<String,String> values = jsonMapper.readValue(line, new TypeReference<Map<String,String>>() { });
			return tupleFactory_.newTuple(values);
		} catch (JsonParseException e) {
			LOG.warn("Failed to parse JSON on line: " + line, e);
			return null;
		} catch (JsonMappingException e) {
			LOG.warn("Failed to map JSON on line: " + line, e);
			return null;
		} catch (IOException e) {
			LOG.warn("IOException on line: " + line, e);
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
		in = (LineRecordReader) reader;
	}

	@Override
	public void setLocation(String location, Job job) throws IOException {
		PigFileInputFormat.setInputPaths(job, location);
	}
}