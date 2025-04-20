package com.demo.topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义key的排序器
 */
public class TopNComparator extends WritableComparator {

    public TopNComparator() {
        super(TopNKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TopNKey ta = (TopNKey) a;
        TopNKey tb = (TopNKey) b;
        //按照年月正序排
        int c1 = Integer.compare(ta.getYear(), tb.getYear());
        if (c1 == 0) {
            int c2 = Integer.compare(ta.getMonth(), tb.getMonth());
            if (c2 == 0) {
                //如果年月日相同，则根据温度倒序排
                return -Integer.compare(ta.getTemperature(),tb.getTemperature());
            }
            return c2;
        }
        return c1;
    }
}
