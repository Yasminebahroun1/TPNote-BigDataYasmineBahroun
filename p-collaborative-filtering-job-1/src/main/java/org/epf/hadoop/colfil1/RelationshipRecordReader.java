package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.epf.hadoop.colfil1.Relationship;

import java.io.IOException;

/**
 * RecordReader personnalisé pour lire des relations à partir d'un fichier d'entrée.
 * Il convertit chaque ligne du fichier en une instance de la classe Relationship.
 */

public class RelationshipRecordReader extends RecordReader<LongWritable, Relationship> {
    private LineRecordReader lineRecordReader = new LineRecordReader();
    private LongWritable currentKey = new LongWritable();
    private Relationship currentValue = new Relationship();

    /**
     * Initialise le RecordReader en préparant la lecture des lignes du fichier.
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        lineRecordReader.initialize(split, context);
    }

    /**
     * Passe à la prochaine paire clé/valeur si elle existe.
     * Découpe la ligne en utilisant le délimiteur "<->" pour identifier les deux utilisateurs liés.
     */

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!lineRecordReader.nextKeyValue()) {
            return false;
        }

        currentKey.set(lineRecordReader.getCurrentKey().get());
        // Découpe la ligne pour obtenir les utilisateurs liés.
        String[] tokens = lineRecordReader.getCurrentValue().toString().split("<->");
        currentValue.setId1(tokens[0].trim());
        currentValue.setId2(tokens[1].split(";")[0].trim());
        return true;
    }
    /**
     * Retourne la clé actuelle (numéro de ligne).
     */
    @Override
    public LongWritable getCurrentKey() {
        return currentKey;
    }

    /**
     * Retourne la valeur actuelle (relation entre deux utilisateurs).
     */
    @Override
    public Relationship getCurrentValue() {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException {
        return lineRecordReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        lineRecordReader.close();
    }
}
