package org.epf.hadoop.colfil3;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ColFilJob3 extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        // Vérification du nombre d'arguments
        if (args.length != 2) {
            System.err.println("Usage: ColFilJob3 <input path> <output path>");
            return -1;
        }
        System.out.println("Argument 0 (Input Path): " + args[0]);
        System.out.println("Argument 1 (Output Path): " + args[1]);

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Collaborative Filtering - Job 3");

        job.setJarByClass(ColFilJob3.class);
        job.setMapperClass(UserSimilarityMapper.class);
        job.setReducerClass(UserSimilarityReducer.class);

        job.setMapOutputKeyClass(UserPairWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(UserPairWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new ColFilJob3(), args);
        System.exit(exitCode);
    }
}
