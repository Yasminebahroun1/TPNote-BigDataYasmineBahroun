package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
// Classe Reducer du Job 2 pour compter les connexions indirectes entre les utilisateurs
public class RelationshipReducer2 extends Reducer<UserPair, IntWritable, UserPair, IntWritable> {
    private IntWritable result = new IntWritable();

    @Override
    protected void reduce(UserPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        // Parcourt toutes les valeurs associées à une paire d'utilisateurs (UserPair)
        for (IntWritable val : values) {
            sum += val.get();  // Incrémente la somme des connexions indirectes
        }
        result.set(sum);
        // Émet la paire d'utilisateurs et le nombre total de connexions indirectes
        context.write(key, result);
    }
}
