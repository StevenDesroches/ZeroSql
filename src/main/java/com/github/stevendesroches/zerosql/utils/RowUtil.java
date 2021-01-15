package com.github.stevendesroches.zerosql.utils;

import java.util.HashMap;
import java.util.Map;

public class RowUtil {
    public final static Map<String, Class> ROW_TYPE = new HashMap<>();

    private RowUtil() {
    }

    static {
        ROW_TYPE.put("INT", int.class);
        ROW_TYPE.put("String", String.class);
    }
}
