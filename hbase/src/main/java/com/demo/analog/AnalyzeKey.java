package com.demo.analog;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;
import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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
        if (this.name.contentEquals(o.name)
                && this.time.contentEquals(o.time)) {
            return 0;
        }
        return 1;
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
}