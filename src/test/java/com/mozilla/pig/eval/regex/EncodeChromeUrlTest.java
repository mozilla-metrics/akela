package com.mozilla.pig.eval.regex;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.Calendar;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import com.mozilla.pig.eval.date.FormatDate;

public class EncodeChromeUrlTest {

    private EncodeChromeUrl encoder = new EncodeChromeUrl();
    private TupleFactory tupleFactory = TupleFactory.getInstance();
    
    @Test
    public void testExec1() throws IOException {
        String outputDateStr = encoder.exec(null);
        assertNull(outputDateStr);
    }

    @Test
    public void testExec2() throws IOException {
        Tuple input = tupleFactory.newTuple();
        String outputDateStr = encoder.exec(input);
        assertNull(outputDateStr);
    }

    @Test
    public void testExec3() throws IOException {
        Tuple input = tupleFactory.newTuple();
        String inputStr = "foochrome://global/locale/intl.propertiesbar";
        input.append(inputStr);
        String encodedStr = URLEncoder.encode(inputStr, "UTF-8");
        
        String outputStr = encoder.exec(input);
        assertEquals(encodedStr, outputStr);
    }
    
}
