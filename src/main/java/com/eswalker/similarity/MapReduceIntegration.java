package com.eswalker.similarity;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapReduceIntegration extends Configured implements Tool {


    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new MapReduceIntegration(), args);
        System.exit(status);
    }

    @Override
    public int run(String[] args) throws Exception {

        Options options = new Options();
        options.addOption("input", true, "input path");
        options.addOption("output", true, "output path");
        options.addOption("alphabet", true, "alphabet");
        options.addOption("threshold", true, "threshold edit distance");

        CommandLineParser parser = new PosixParser();
        CommandLine c = parser.parse(options, args);

        String in = c.getOptionValue("input");
        String out = c.getOptionValue("output");
        String alpha = c.getOptionValue("alphabet");
        String thresh = c.getOptionValue("threshold");

        Configuration conf = getConf();
        if (alpha != null)
            conf.set("alphabet", alpha);
        if (thresh != null)
            conf.setInt("threshold", Integer.parseInt(thresh));

        Job job = new Job(conf);
        job.setJarByClass(MapReduceIntegration.class);
        job.setJobName("MapReduceIntegration");
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPaths(job, in);
        TextOutputFormat.setOutputPath(job, new Path(out));
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
      //  job.setOutputKeyClass(NullWritable.class);
     //   job.setOutputValueClass(Text.class);
        return job.waitForCompletion(true) ? 0 : 1;

    }
}