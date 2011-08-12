package com.mozilla.pig.filter.map;

import java.io.IOException;
import java.util.Map;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

/**
 * Determines if the given map has contains the given key
 */
public class ContainsKey extends FilterFunc  {

    @SuppressWarnings("rawtypes")
    @Override
    public Boolean exec(Tuple input) throws IOException {
        if (input == null || input.size() < 2) {
            return false;
        }
        
        Map m = (Map)input.get(0);
        Object k = input.get(1);
        
        return m.containsKey(k);
    }

}
