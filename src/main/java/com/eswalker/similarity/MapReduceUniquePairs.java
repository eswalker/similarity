package com.eswalker.similarity;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashSet;

public class MapReduceUniquePairs extends Configured implements Tool {

    public static class MapUnique extends Mapper<LongWritable, Text, LongWritable, Text> {

        private LongWritable k = new LongWritable();
        private Text v = new Text();

        @Override
        protected void map(LongWritable key, Text record, Context context)
                throws IOException, InterruptedException {

            long l = record.toString().hashCode();
            k.set(l);
            v.set(record.toString());
            context.write(k, v);

        }

    }

    public static class ReduceUnique extends Reducer<LongWritable, Text, NullWritable, Text> {

        private NullWritable k = NullWritable.get();
        private Text v = new Text();

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {


            HashSet<String> set = new HashSet<String>();

            for (Text t : values) {
               set.add(t.toString());
            }

            for (String s : set) {
                v.set(s);
                context.write(k,v);
            }

        }


    }

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new MapReduceUniquePairs(), args);
        System.exit(status);
    }

    @Override
    public int run(String[] args) throws Exception {

        Options options = new Options();
        options.addOption("input", true, "input path");
        options.addOption("output", true, "output path");

        CommandLineParser parser = new PosixParser();
        CommandLine c = parser.parse(options, args);

        String in = c.getOptionValue("input");
        String out = c.getOptionValue("output");

        Configuration conf = getConf();

        Job job = new Job(conf);
        job.setJarByClass(MapReduceUniquePairs.class);
        job.setJobName("MapReduceUniquePairs");
        job.setMapperClass(MapUnique.class);
        job.setReducerClass(ReduceUnique.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPaths(job, in);
        TextOutputFormat.setOutputPath(job, new Path(out));
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;

    }
}