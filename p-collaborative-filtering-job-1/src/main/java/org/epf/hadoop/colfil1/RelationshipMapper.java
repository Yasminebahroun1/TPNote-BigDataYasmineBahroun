package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

/**
 * Mapper pour le premier job Hadoop.
 * Son rôle est de lire les relations entre utilisateurs et d'émettre les paires d'utilisateurs.
 */

public class RelationshipMapper extends Mapper<LongWritable, Relationship, Text, Text> {
    private Text user1 = new Text();
    private Text user2 = new Text();

    /**
     * Méthode principale du Mapper qui est appelée pour chaque ligne du fichier d'entrée.
     * Elle émet deux sorties : (user1, user2) et (user2, user1), pour représenter la relation dans les deux sens.
     */

    @Override
    protected void map(LongWritable key, Relationship value, Context context) throws IOException, InterruptedException {
        // Récupération des identifiants des deux utilisateurs liés.
        user1.set(value.getId1());
        user2.set(value.getId2());
        // Émission de la relation dans les deux sens pour refléter une relation bidirectionnelle.
        context.write(user1, user2);
        context.write(user2, user1);
    }
}
