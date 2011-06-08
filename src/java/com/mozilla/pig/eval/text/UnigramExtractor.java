package com.mozilla.pig.eval.text;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class UnigramExtractor extends EvalFunc<DataBag> {

	private static BagFactory bagFactory = BagFactory.getInstance();
	private static TupleFactory tupleFactory = TupleFactory.getInstance();
	
	private static final Pattern spacePattern = Pattern.compile("\\s+");
	private static final Pattern punctPattern = Pattern.compile("\\p{Punct}(?:(?<!\\d)(?!\\d))");
	
	public DataBag exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return null;
		}

		String normStr = ((String)input.get(0));
		if (normStr == null) {
			return null;
		}

		// Remove punctuation except when it's a version number
		normStr = punctPattern.matcher(normStr.trim().toLowerCase()).replaceAll(" ");
		normStr = spacePattern.matcher(normStr).replaceAll(" ");
		
		DataBag output = bagFactory.newDefaultBag();
		for (String s : spacePattern.split(normStr.trim())) {
			if (s.length() <= 30) {
				Tuple t = tupleFactory.newTuple(1);
	            t.set(0, s);
				output.add(t);
			}
		}
		
		return output;
	}
}
