package com.eswalker.similarity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;


public class TestMapReduceIntegration {

    private MapDriver<LongWritable, Text, LongWritable, Text> mapDriver;
    private ReduceDriver<LongWritable, Text, NullWritable, Text> reduceDriver;


    @Before
    public void setUp() {
        Map mapper = new Map();
        mapDriver = MapDriver.newMapDriver(mapper);

        Reduce reducer = new Reduce();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);


    }
    @Test
    public void testMapper() throws IOException {
        String input = "1|1";
        String output = input;

        Configuration conf = new Configuration();
        conf.setBoolean("testing", true);
        conf.setInt("threshold", 1);
        conf.set("alphabet", "12");
        mapDriver.withConfiguration(conf);
        mapDriver.withInput(new LongWritable(), new Text(input));

        mapDriver.withOutput(new LongWritable(0), new Text(output));
        mapDriver.withOutput(new LongWritable(1), new Text(output));
        mapDriver.withOutput(new LongWritable(32), new Text(output));
        mapDriver.withOutput(new LongWritable(62), new Text(output));
        mapDriver.withOutput(new LongWritable(31), new Text(output));
        mapDriver.runTest(false);
    }

    @Test
    public void testReducer() throws IOException {
        Configuration conf = new Configuration();
        conf.setBoolean("testing", true);
        conf.setInt("threshold", 1);

        ArrayList<Text> texts = new ArrayList();
        texts.add(new Text(" 1|1"));
        texts.add(new Text(" 2|12"));
        texts.add(new Text(" 3|111"));

        reduceDriver.withConfiguration(conf);
        reduceDriver.withInput(new LongWritable(1), texts);
        reduceDriver.withOutput(NullWritable.get(), new Text("1|1|2|1|12"));

        reduceDriver.runTest(false);

    }


}
