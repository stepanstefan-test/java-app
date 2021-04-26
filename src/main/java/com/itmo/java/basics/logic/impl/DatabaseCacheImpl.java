package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.DatabaseCache;

import java.util.Iterator;
import java.util.LinkedHashMap;

public class DatabaseCacheImpl implements DatabaseCache {

    private static final int MAX_CACHE_SIZE = 5_000;

    private final LinkedHashMap<String, byte[]> cacheMap = new LinkedHashMap<>(MAX_CACHE_SIZE);

    @Override
    public byte[] get(String key) {
        var value = cacheMap.get(key);

        if (value != null) {
            set(key, value);
        }

        return value;
    }

    @Override
    public void set(String key, byte[] value) {
        if (cacheMap.containsKey(key)) {
            delete(key);
        } else if (cacheMap.size() == MAX_CACHE_SIZE) {
            Iterator<String> it = cacheMap.keySet().iterator();
            it.next();
            it.remove();
        }

        cacheMap.put(key, value);
    }

    @Override
    public void delete(String key) {
        cacheMap.remove(key);
    }
}
