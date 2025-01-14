package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import java.io.IOException;
// Classe Mapper du Job 2 pour émettre des paires d'utilisateurs connectés
public class RelationshipMapper2 extends Mapper<Object, Text, UserPair, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private UserPair userPair = new UserPair();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Découpe la ligne en utilisateurs séparés par une virgule
        String[] users = value.toString().split(",");
        // Parcours des paires possibles d'utilisateurs
        for (int i = 0; i < users.length; i++) {
            for (int j = i + 1; j < users.length; j++) {
                // Crée une paire (UserPair) avec les deux utilisateurs
                userPair = new UserPair(users[i].trim(), users[j].trim());
                // Émet la paire avec la valeur 1, indiquant qu'ils sont connectés via un ami commun
                context.write(userPair, one);
            }
        }
    }
}
