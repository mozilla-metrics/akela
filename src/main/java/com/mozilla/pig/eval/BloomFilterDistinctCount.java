package com.mozilla.pig.eval;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

/**
 * BloomFilterDistinctCount is designed to be a way to get a count of distinct items
 * as they pass through while using a minimal amount of memory. This should only be used if 
 * you have no other choice and if you are have out of memory issues during bag spills.
 * 
 * You can compensate somewhat, for the probabilistic nature of bloom filters by adjusting your
 * counts after the fact based on your n and p values. You should also keep in mind that the lesser
 * the p value; you will gain more accuracy, but at a considerable CPU cost if you have more than a
 * few hashes to perform.
 * 
 * Refer to {@link http://hur.st/bloomfilter} calculator as a way to see the effects of n and p values.
 * Take particular note of what the resulting k value is. If its greater than 10 you are likely entering
 * a world of pain.
 * 
 * Example for expecting 1 million items at a 0.001 probablity of a false positive:
 * 
 * define DistinctCount com.mozilla.pig.eval.BloomFilterDistinctCount(1000000, 0.001);
 * 
 */
public class BloomFilterDistinctCount extends EvalFunc<Integer> {

    private int n;
    private double p;
    
    public BloomFilterDistinctCount(String n, String p) {
        this.n = Integer.parseInt(n);
        this.p = Double.parseDouble(p);
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

}
