package com.huawei.www.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class MRTest extends Configured implements Tool{
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MRTest(),args));
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration config = HBaseConfiguration.create();
        Job job=Job.getInstance(config,"hbase mr");
        job.setJarByClass(MRTest.class);

        Scan scan =new Scan();
        TableMapReduceUtil.initTableMapperJob(
                "ns4:t1",        // input table
                scan,               // Scan instance to control CF and attribute selection
                MyHbaseMapper.class,     // mapper class
                ImmutableBytesWritable.class,         // mapper output key
                Put.class,  // mapper output value
                job);

        TableMapReduceUtil.initTableReducerJob(
                "ns5:t1",null,job
        );

       return job.waitForCompletion(true)?0:1;
    }
}
