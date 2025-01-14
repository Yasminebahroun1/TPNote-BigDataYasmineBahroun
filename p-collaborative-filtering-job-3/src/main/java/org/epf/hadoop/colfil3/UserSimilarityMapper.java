package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
// Mapper du Job 3 pour calculer la similarité entre paires d'utilisateurs
public class UserSimilarityMapper extends Mapper<LongWritable, Text, UserPairWritable, IntWritable> {
    private UserPairWritable userPair = new UserPairWritable();// Paire d'utilisateurs émise en tant que clé
    private final IntWritable one = new IntWritable(1);// Valeur par défaut pour indiquer la présence d'une connexion


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Découper chaque ligne du fichier d'entrée en utilisant le caractère tabulation "\t"
        String[] fields = value.toString().split("\t");
        // Vérifie que la ligne contient bien deux champs : la paire d'utilisateurs et la fréquence de connexion
        if (fields.length == 2) {
            // Découpe la paire d'utilisateurs (format "user1,user2")
            String[] users = fields[0].split(",");
            // Définit la paire d'utilisateurs (clé) avec user1 et user2
            userPair.set(users[0], users[1]);
            // Émet la paire d'utilisateurs avec le nombre de connexions
            context.write(userPair, new IntWritable(Integer.parseInt(fields[1])));
        }
    }
}
