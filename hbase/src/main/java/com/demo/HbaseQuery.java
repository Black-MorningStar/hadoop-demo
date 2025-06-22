package com.demo;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class HbaseQuery {

    private byte[] rowKey;
    private List<QueryDetail> details;
    @Getter
    @Setter
    public static class QueryDetail {
        private byte[] columnFamily;
        private byte[] columnName;
    }
}
