package com.demo;

import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义key的排序器
 */
public class TopNComparator extends WritableComparator {

    public TopNComparator() {
        super(TopNKey.class,true);
    }


}
