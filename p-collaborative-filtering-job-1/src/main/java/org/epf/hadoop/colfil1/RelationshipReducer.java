package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
/**
 * Reducer pour regrouper les connexions d'un utilisateur en une seule liste.
 * Chaque clé (utilisateur) est associée à la liste de ses amis.
 */
public class RelationshipReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> friends = new ArrayList<>();
        // Parcourt toutes les valeurs (les amis de l'utilisateur) et les ajoute à la liste.
        for (Text val : values) {
            friends.add(val.toString());
        }
        // Convertit la liste en une chaîne de caractères séparée par des virgules.
        String output = String.join(",", friends);
        context.write(key, new Text(output));
    }
}
