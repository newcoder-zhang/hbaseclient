package com.huawei.www.mr;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyHbaseRed extends TableReducer<ImmutableBytesWritable,Put,ImmutableBytesWritable> {
    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
    }
}
