package com.eswalker.similarity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map extends Mapper<LongWritable, Text, LongWritable, Text> {

    private LongWritable k = new LongWritable();
    private Text v = new Text();
    private NewEditDistance ned;
    private int threshold;

    @Override
    protected final void setup(final Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String alphabet = conf.get("alphabet", " ~");
        char lowChar = alphabet.charAt(0);
        char highChar = alphabet.charAt(alphabet.length() - 1);

        ned = new NewEditDistance(lowChar,highChar);
        threshold = conf.getInt("threshold", 2);
    }

    @Override
    protected void map(LongWritable key, Text record, Context context)
            throws IOException, InterruptedException {

        v.set(record.toString());
        String[] data = record.toString().split("\\|", 2);
        String element = data[1];
        ned.addBuckets(element, threshold);

        for (Long l : ned.getBuckets()) {
            k.set(l);
            context.write(k,v);
        }

        ned.clearBuckets();

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

    }
}