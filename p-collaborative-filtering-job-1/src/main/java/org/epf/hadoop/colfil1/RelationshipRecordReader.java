package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.epf.hadoop.colfil1.Relationship;

import java.io.IOException;

public class RelationshipRecordReader extends RecordReader<LongWritable, Relationship> {
    private LineRecordReader lineRecordReader = new LineRecordReader();
    private LongWritable currentKey = new LongWritable();
    private Relationship currentValue = new Relationship();

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        lineRecordReader.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!lineRecordReader.nextKeyValue()) {
            return false;
        }

        currentKey.set(lineRecordReader.getCurrentKey().get());
        String[] tokens = lineRecordReader.getCurrentValue().toString().split("<->");
        currentValue.setId1(tokens[0].trim());
        currentValue.setId2(tokens[1].split(";")[0].trim());
        return true;
    }

    @Override
    public LongWritable getCurrentKey() {
        return currentKey;
    }

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
