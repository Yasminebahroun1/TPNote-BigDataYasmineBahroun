package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.RecordReader;

import java.io.IOException;

public class RelationshipInputFormat extends FileInputFormat<LongWritable, Relationship> {

    /**
     * Cette méthode crée un RecordReader qui va interpréter les lignes du fichier d'entrée
     * et les transformer en objets Relationship (relations entre utilisateurs).
     */

    @Override
    public RecordReader<LongWritable, Relationship> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        // On crée le RecordReader personnalisé pour lire les relations.
        RelationshipRecordReader reader = new RelationshipRecordReader();
        // Initialisation du reader avec le morceau de fichier (split) à traiter.
        reader.initialize(split, context);
        return reader;
    }
}