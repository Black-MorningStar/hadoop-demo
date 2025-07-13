package com.demo.base;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class HbaseData {

    private byte[] rowKey;
    private List<HbaseDataDetail> data;

    @Getter
    @Setter
    public static class HbaseDataDetail {
        private byte[] columnFamily;
        private byte[] columnName;
        private byte[] columnValue;
    }
}
