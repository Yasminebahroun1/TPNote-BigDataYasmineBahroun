package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UserSimilarityMapper extends Mapper<LongWritable, Text, UserPairWritable, IntWritable> {
    private UserPairWritable userPair = new UserPairWritable();
    private final IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        if (fields.length == 2) {
            String[] users = fields[0].split(",");
            userPair.set(users[0], users[1]);
            context.write(userPair, new IntWritable(Integer.parseInt(fields[1])));
        }
    }
}
