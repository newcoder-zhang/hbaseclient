package com.huawei.www.mr;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * input  row,result
 * output row put
 */
public class MyHbaseMapper extends TableMapper<ImmutableBytesWritable, Put> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        byte[] rowkey = key.get();
        Cell[] cells = value.rawCells();
        Put put = new Put(rowkey);
        for (Cell cell : cells) {
            byte[] family = CellUtil.cloneFamily(cell);
            byte[] qual = CellUtil.cloneQualifier(cell);

            if (Bytes.toString(family).equals("info") && Bytes.toString(qual).equals("name")) {
                put.add(cell);
            }
        }
        context.write(key, put);
    }
}
