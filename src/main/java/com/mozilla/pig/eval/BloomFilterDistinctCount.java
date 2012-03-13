package com.mozilla.pig.eval;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

public class BloomFilterDistinctCount extends EvalFunc<Integer> {

    private int n;
//    private int k;
//    private int m;
    private double p;
    
    public BloomFilterDistinctCount(String n, String p) {
        this.n = Integer.parseInt(n);
        this.p = Double.parseDouble(p);
//        int m = (int)Math.ceil((n * Math.log(p)) / Math.log(1.0 / (Math.pow(2.0, Math.log(2.0)))));
//        k = (int)Math.round(Math.log(2.0) * m / n);
        
    }
    
    @Override
    public Integer exec(Tuple input) throws IOException {
        if (input.size() != 1) {
            throw new RuntimeException("Expected input to have only a single field");
        }
        if (input.getType(0) != DataType.BAG) {
            throw new RuntimeException("Expected a BAG as input");
        }

        // guava bloom
        BloomFilter<CharSequence> filter = BloomFilter.create(Funnels.stringFunnel(), n, p);
        // hadoop bloom
        //BloomFilter filter = new BloomFilter(m, k, Hash.MURMUR_HASH);
        int uniq = 0;
        
        DataBag db = (DataBag) input.get(0);
        for (Iterator<Tuple> iter = db.iterator(); iter.hasNext();) {
            Tuple t = iter.next();
            if (!filter.mightContain((String)t.get(0))) {
                filter.put((String)t.get(0));
                //filter.add(t);
                uniq++;
            }
        }

        return uniq;
    }
    
//    public static void main(String[] args) {
//        BloomFilter<CharSequence> filter = BloomFilter.create(Funnels.stringFunnel(), 10000, 0.000001d);
//        Set<String> added = new HashSet<String>();
//        Set<String> notAdded = new HashSet<String>();
//        int uniq = 0;
//        int n = 20000;
//        for (int i=0; i < n; i++) {
//            String id = UUID.randomUUID().toString();
//            if (!filter.mightContain(id)) {
//                filter.put(id.toString());
//                uniq++;
//                added.add(id);
//            } else {
//                notAdded.add(id);
//            }
//        }
//        
//        for (int i=0; i < n; i++) {
//            notAdded.add(UUID.randomUUID().toString());
//        }
//        
//        System.out.println(String.format("uniq[%d] added.size[%d] notAdded.size[%d]", uniq, added.size(), notAdded.size()));
//        
//        for (String a : added) {
//            if (!filter.mightContain(a)) {
//                System.out.println("filter thinks it does not contain: " + a);
//            }
//        }
//        for (String na : notAdded) {
//            if (filter.mightContain(na)) {
//                System.out.println("filter thinks it contains: " + na);
//            }
//        }
//    }
}
