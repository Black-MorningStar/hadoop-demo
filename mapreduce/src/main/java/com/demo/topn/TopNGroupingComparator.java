package com.demo.topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TopNGroupingComparator extends WritableComparator {

    public TopNGroupingComparator() {
        super(TopNKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TopNKey ta = (TopNKey) a;
        TopNKey tb = (TopNKey) b;
        //想同年月则为一组
        if (ta.getYear() == tb.getYear() && ta.getMonth() == tb.getMonth()) {
            return 0;
        }
        return 1;
    }

}
