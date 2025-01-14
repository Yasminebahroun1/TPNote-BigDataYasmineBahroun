package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class RelationshipReducer2 extends Reducer<UserPair, IntWritable, UserPair, IntWritable> {
    private IntWritable result = new IntWritable();

    @Override
    protected void reduce(UserPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}
