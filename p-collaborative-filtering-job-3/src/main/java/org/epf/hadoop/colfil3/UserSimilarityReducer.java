package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
// Reducer du Job 3 pour calculer la somme des similarités pour chaque paire d'utilisateurs
public class UserSimilarityReducer extends Reducer<UserPairWritable, IntWritable, UserPairWritable, IntWritable> {

    @Override
    protected void reduce(UserPairWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0; // Variable pour stocker la somme des connexions indirectes
        for (IntWritable val : values) {
            sum += val.get();
        }
        // Écrit la paire d'utilisateurs et la somme totale des connexions indirectes
        context.write(key, new IntWritable(sum));
    }
}
