package com.github.stevendesroches.zerosql;

import com.github.stevendesroches.zerosql.utils.RowUtil;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/*
 * Original Source : https://stackoverflow.com/questions/16882971/retrieve-entire-row-with-resultset
 */

public class Row {
    private Map<String, Entry<Class, Object>> row;

    public Row() {
        row = new HashMap<>();
    }

    public void add(String column, Object data, String type) {
        Class castType = RowUtil.TYPE.get(type.toUpperCase());
        try {
            this.add(column, castType.cast(data));
        } catch (NullPointerException e) {
            e.printStackTrace();
        }
    }

    private <T> void add(String column, T data) {
        row.put(column, new AbstractMap.SimpleEntry<>(data.getClass(), data));
    }

    public Object getVal(String column) {
        Object result = null;
        if (row != null) {
            result = row.get(column).getValue();
        }
        return result;
    }

    public Class getClass(String column) {
        Class result = null;
        if (row != null) {
            result = row.get(column).getKey();
        }
        return result;
    }

    public Entry get(String column) {
        Entry result = null;
        if (row != null) {
            result = row.get(column);
        }
        return result;
    }
}
