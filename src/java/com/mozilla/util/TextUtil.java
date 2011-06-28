package com.mozilla.util;

import java.util.regex.Pattern;

public class TextUtil {

    private static final Pattern spacePattern = Pattern.compile("\\s+");
    private static final Pattern punctPattern = Pattern.compile("\\p{Punct}(?:(?<!\\d)(?!\\d))");
    
    public static String normalize(String s, boolean lowerCase) {
        String normStr = lowerCase ? s.trim().toLowerCase() : s.trim();
        normStr = punctPattern.matcher(normStr).replaceAll(" ");
        normStr = spacePattern.matcher(normStr).replaceAll(" ");
        
        return normStr.trim();
    }
    
    public static String[] tokenize(String s) {
        return spacePattern.split(s);
    }
    
}
