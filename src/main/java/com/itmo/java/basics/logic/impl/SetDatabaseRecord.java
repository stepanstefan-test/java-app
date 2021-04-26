package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

public class SetDatabaseRecord implements WritableDatabaseRecord {

    private static final int INTEGER_SIZE = 4;

    private final byte[] key;
    private final byte[] value;

    public SetDatabaseRecord(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public byte[] getValue() {
        return value;
    }

    @Override
    public long size() {
        return INTEGER_SIZE + key.length + INTEGER_SIZE + value.length;
    }

    @Override
    public boolean isValuePresented() {
        return value != null;
    }

    @Override
    public int getKeySize() {
        return key.length;
    }

    @Override
    public int getValueSize() {
        return value.length;
    }
}
