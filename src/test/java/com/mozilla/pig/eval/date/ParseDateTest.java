package com.mozilla.pig.eval.date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

public class ParseDateTest {

    private ParseDate parseDate = new ParseDate();
    private TupleFactory tupleFactory = TupleFactory.getInstance();
    
    @Test
    public void testExec1() throws IOException {
        Long millis = parseDate.exec(null);
        assertNull(millis);
    }

    @Test
    public void testExec2() throws IOException {
        Tuple input = tupleFactory.newTuple();
        Long millis = parseDate.exec(input);
        assertNull(millis);
    }

    @Test
    public void testExec3() throws IOException {
        Tuple input = tupleFactory.newTuple();
        
        Calendar cal = Calendar.getInstance();
        long inputTimeMillis = cal.getTimeInMillis();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss:S");
        
        input.append("yyyyMMdd HH:mm:ss:S");
        input.append(sdf.format(cal.getTime()));
        
        Long outputTimeMillis = parseDate.exec(input);
        assertNotNull(outputTimeMillis);
        assertEquals(inputTimeMillis, (long)outputTimeMillis);
    }
    
}
