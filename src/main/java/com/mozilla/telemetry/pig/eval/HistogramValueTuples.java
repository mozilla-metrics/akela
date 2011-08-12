package com.mozilla.telemetry.pig.eval;

import java.io.IOException;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class HistogramValueTuples extends EvalFunc<DataBag> {

    private static BagFactory bagFactory = BagFactory.getInstance();
    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    
    @SuppressWarnings("unchecked")
    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        
        DataBag output = bagFactory.newDefaultBag();
        Map<String,Map<String,Map<String,Object>>> m = (Map<String,Map<String,Map<String,Object>>>)input.get(0);
        if (m != null) {
            for (Map.Entry<String, Map<String,Map<String,Object>>> hist : m.entrySet()) {
                Map<String,Map<String,Object>> hv = hist.getValue();
                if (hv != null) {
                    Map<String,Object> values = hv.get("values");
                    if (values != null) {
                        for (Map.Entry<String, Object> v : values.entrySet()) {
                            Tuple t = tupleFactory.newTuple(3);
                            t.set(0, hist.getKey());
                            t.set(1, v.getKey());
                            t.set(2, v.getValue());
                            output.add(t);
                        }
                    }
                }
            }
        }
        
        return output;
    }

}
