package com.mozilla.pig.eval.json;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Map;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import com.mozilla.telemetry.pig.eval.HistogramValueTuples;

public class JsonMapTest {

    private JsonMap jsonMap = new JsonMap();
    private TupleFactory tupleFactory = TupleFactory.getInstance();
    
    private String readFile(String file) throws IOException {
        StringBuilder sb = new StringBuilder();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
            String line = null;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
        
        return sb.toString();
    }
    
    @Test
    public void testExec1() throws IOException {
        Tuple input = tupleFactory.newTuple();
        input.append("{ \"foo\": \"bar\" }");
        Map<String,Object> myMap = jsonMap.exec(input);
        assertTrue(myMap.containsKey("foo"));
        assertEquals(myMap.get("foo"), "bar");
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testExec2() throws IOException {
        Tuple input = tupleFactory.newTuple();
        input.append(readFile(System.getProperty("basedir") + "/src/test/resources/telemetry.js"));
        Map<String,Object> myMap = jsonMap.exec(input);
        Map<String,Object> histograms = (Map<String,Object>)myMap.get("histograms");
        
        assertTrue(histograms.containsKey("MOZ_SQLITE_OTHER_SYNC_MAIN_THREAD_MS"));
    }
    
}
