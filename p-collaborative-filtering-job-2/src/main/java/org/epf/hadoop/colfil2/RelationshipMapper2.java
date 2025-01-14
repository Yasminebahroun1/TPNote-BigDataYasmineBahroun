package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class RelationshipMapper2 extends Mapper<Object, Text, UserPair, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private UserPair userPair = new UserPair();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] users = value.toString().split(",");
        for (int i = 0; i < users.length; i++) {
            for (int j = i + 1; j < users.length; j++) {
                userPair = new UserPair(users[i].trim(), users[j].trim());
                context.write(userPair, one);
            }
        }
    }
}
