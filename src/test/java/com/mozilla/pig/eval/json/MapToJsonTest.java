package com.mozilla.pig.eval.json;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

public class MapToJsonTest {

    private MapToJson m2json = new MapToJson();    
    private TupleFactory tupleFactory = TupleFactory.getInstance();
    
    @Test
    public void testExec1() throws IOException {
        Tuple input = tupleFactory.newTuple();
        Map<String,String> myMap = new HashMap<String,String>();
        myMap.put("foo", "bar");
        input.append(myMap);
        
        String jsonStr = m2json.exec(input);
        assertNotNull(jsonStr);
        assertEquals("{\"foo\":\"bar\"}", jsonStr);
    }
    
    @Test
    public void testExec2() throws IOException {
        Tuple input = tupleFactory.newTuple();
        Map<String,Object> myMap = new HashMap<String,Object>();
        myMap.put("fruit", new String[] { "apple", "orange" });
        input.append(myMap);
        
        String jsonStr = m2json.exec(input);
        assertNotNull(jsonStr);
        assertEquals("{\"fruit\":[\"apple\",\"orange\"]}", jsonStr);
    }
    
    @Test
    public void testExec3() throws IOException {
        Tuple input = tupleFactory.newTuple();
        Map<String,Object> myMap = new HashMap<String,Object>();
        myMap.put("numStr", "1");
        input.append(myMap);
        
        String jsonStr = m2json.exec(input);
        assertNotNull(jsonStr);
        assertEquals("{\"numStr\":\"1\"}", jsonStr);
    }
    
    @Test
    public void testExec4() throws IOException {
        Tuple input = tupleFactory.newTuple();
        Map<String,Object> myMap = new HashMap<String,Object>();
        myMap.put("actualNum", 1L);
        input.append(myMap);
        
        String jsonStr = m2json.exec(input);
        assertNotNull(jsonStr);
        assertEquals("{\"actualNum\":1}", jsonStr);
    }
}
