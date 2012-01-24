package com.mozilla.pig.eval.date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.Calendar;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

public class FormatDateTest {

    private FormatDate formatDate = new FormatDate();
    private TupleFactory tupleFactory = TupleFactory.getInstance();
    
    @Test
    public void testExec1() throws IOException {
        String outputDateStr = formatDate.exec(null);
        assertNull(outputDateStr);
    }

    @Test
    public void testExec2() throws IOException {
        Tuple input = tupleFactory.newTuple();
        String outputDateStr = formatDate.exec(input);
        assertNull(outputDateStr);
    }

    @Test
    public void testExec3() throws IOException {
        Tuple input = tupleFactory.newTuple();
        
        Calendar cal = Calendar.getInstance();
        cal.set(2011, Calendar.JANUARY, 23);
        
        input.append("yyyyMMdd");
        input.append(cal.getTimeInMillis());
        
        String outputDateStr = formatDate.exec(input);
        assertNotNull(outputDateStr);
        assertEquals(outputDateStr, "20110123");
    }
}
