package com.github.stevendesroches.zerosql;

import com.github.stevendesroches.zerosql.utils.RowUtil;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class Row {
    private Map<String, Entry<Class, Object>> row;

    public Row() {
        row = new HashMap<>();
    }

    public void add(String column, Object data, String type) {
        Class castType = RowUtil.ROW_TYPE.get(type.toUpperCase());
        try {
            this.add(column, castType.cast(data));
        } catch (NullPointerException e) {
            e.printStackTrace();
        }
    }

    private <T> void add(String column, T data) {
        row.put(column, new AbstractMap.SimpleEntry<>(data.getClass(), data));
    }

    public Entry get(String column) {
        Entry result = null;
        if (row != null) {
            result = row.get(column);
        }
        return result;
    }
}
