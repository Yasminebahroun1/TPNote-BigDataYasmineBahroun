package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class UserSimilarityReducer extends Reducer<UserPairWritable, IntWritable, UserPairWritable, IntWritable> {

    @Override
    protected void reduce(UserPairWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
