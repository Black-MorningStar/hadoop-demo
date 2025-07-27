package com.demo.analog;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;
import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @Author: 君墨笑
 * @Date: 2025/7/15 12:17
 */
@Getter
@Setter
public class AnalyzeKey implements WritableComparable<AnalyzeKey> {

    public static final String USER = "user";
    public static final String PLAT = "plat";

    public AnalyzeKey() {
    }

    private String name;

    private String time;

    private String type;

    @Override
    public int compareTo(@NotNull AnalyzeKey o) {
        int cmp = this.name.compareTo(o.name);
        if (cmp != 0) return cmp;
        return this.time.compareTo(o.time);
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF( name);
        dataOutput.writeUTF(time);
        dataOutput.writeUTF(type);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        name = dataInput.readUTF();
        time = dataInput.readUTF();
        type = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnalyzeKey that = (AnalyzeKey) o;
        return Objects.equals(name, that.name) && Objects.equals(time, that.time);
    }

    //默认底层使用hash进行分区
    @Override
    public int hashCode() {
        return Objects.hash(name, time);
    }
}