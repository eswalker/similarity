package com.eswalker.similarity;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class Reduce extends Reducer<LongWritable, Text, NullWritable, Text> {

    private NullWritable k = NullWritable.get();
    private Text v = new Text();

    private int threshold;


    @Override
    protected final void setup(final Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        threshold = conf.getInt("threshold", 2);
    }

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {


        ArrayList<String> elements = new ArrayList<String>();
        ArrayList<String> elementIds = new ArrayList<String>();

        for (Text t : values) {
            String data[] = t.toString().split("\\|", 2);
            elements.add(data[1]);
            elementIds.add(data[0].trim());
        }

        long comparisons = 0;
        for (int i = 0; i < elements.size(); i++) {
            for (int j = i + 1; j < elements.size(); j++) {
                comparisons++;
                String a = elements.get(i);
                String b = elements.get(j);
                int aID = Integer.parseInt(elementIds.get(i));
                int bID = Integer.parseInt(elementIds.get(j));

                int edit = StringUtils.getLevenshteinDistance(a, b);

                if (edit <= threshold) {
                    String output = "";
                    if (aID == Math.min(aID, bID)) {
                        output = edit + "|"
                                + aID + "|"
                                + bID + "|"
                                + a + "|"
                                + b;
                    } else {
                        output = edit + "|"
                                + bID + "|"
                                + aID + "|"
                                + b + "|"
                                + a;
                    }
                    v.set(output);
                    context.write(k,v);
                }
            }


        }
        context.getCounter("CUSTOM", "COMPS").increment(comparisons);

    }


}