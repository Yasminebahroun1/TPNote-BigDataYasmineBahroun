package org.epf.hadoop.colfil2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ColFilJob2 {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Error: Expected 2 arguments, but received " + args.length);
            System.err.println("Usage: ColFilJob2 <input path> <output path>");
            for (int i = 0; i < args.length; i++) {
                System.out.println("Received argument " + i + ": " + args[i]);
            }
            System.exit(-1);
        }
        System.out.println("Argument 0 (Input Path): " + args[0]);
        System.out.println("Argument 1 (Output Path): " + args[1]);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Collaborative Filtering - Job 2");

        job.setJarByClass(ColFilJob2.class);
        job.setMapperClass(RelationshipMapper2.class);
        job.setReducerClass(RelationshipReducer2.class);

        // Ajoute 2 reducers
        job.setNumReduceTasks(2);  // Cette ligne ajoute deux reducers

        job.setMapOutputKeyClass(UserPair.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(UserPair.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
