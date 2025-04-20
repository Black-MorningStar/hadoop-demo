package com.demo.topn;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义MapTask输出的Key包装类
 */
@Getter
@Setter
public class TopNKey implements WritableComparable<TopNKey> {

    private int year;

    private int month;

    private int day;

    private int temperature;

    //自定义key排序规则
    @Override
    public int compareTo(TopNKey that) {
        //按照年月正序排
        int c1 = Integer.compare(this.year, that.year);
        if (c1 == 0) {
            int c2 = Integer.compare(this.month, that.month);
            if (c2 == 0) {
                //如果年月相同，则根据温度倒序排
                return -Integer.compare(this.temperature,that.temperature);
            }
            return c2;
        }
        return c1;
    }

    //自定义序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(year);
        dataOutput.writeInt(month);
        dataOutput.writeInt(day);
        dataOutput.writeInt(temperature);
    }

    //自定义反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        //反序列的顺序需要和序列化一样
        this.year = dataInput.readInt();
        this.month = dataInput.readInt();
        this.day = dataInput.readInt();
        this.temperature = dataInput.readInt();
    }
}
