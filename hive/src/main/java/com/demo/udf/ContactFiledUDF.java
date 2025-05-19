package com.demo.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class ContactFiledUDF extends UDF {

    public Text evaluate(Text input1, IntWritable input2) throws UDFArgumentException {
        if (input1 == null || input2 == null) {
            throw new UDFArgumentException("入参不可以为空");
        }
        return new Text(input1.toString() + "-" + input2.get());
    }
}
