package com.eqt.input.sequence.offset;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;


/**
 * allows the original key of the sequence file to come out as well as offset and filename.
 */
public class KeyedOffsetReader<K extends WritableComparable<K>,V extends Writable> extends FileInputFormat<OffsetWrapperWritable<K>, V> {

    private static class KeyedOffsetRecordReader<K extends WritableComparable<K>,V extends Writable> extends RecordReader<OffsetWrapperWritable<K>, V> {

        private SequenceFile.Reader in;
        private long start;
        private long end;
        private boolean more = true;
        private OffsetWrapperWritable<K> key = null;
        private K k = null;
        private V value = null;
        private Configuration conf;

        @SuppressWarnings({ "unchecked", "deprecation" })
		@Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) split;
            conf = context.getConfiguration();
            Path path = fileSplit.getPath();
            FileSystem fs = path.getFileSystem(conf);
            this.in = new SequenceFile.Reader(fs, path, conf);
            try {
                this.k = (K) in.getKeyClass().newInstance();
                this.value = (V) in.getValueClass().newInstance();
            } catch (InstantiationException e) {
                throw new IOException(e);
            } catch (IllegalAccessException e) {
                throw new IOException(e);
            }
            this.end = fileSplit.getStart() + fileSplit.getLength();

            if (fileSplit.getStart() > in.getPosition()) {
                in.sync(fileSplit.getStart());
            }

            this.start = in.getPosition();
            more = start < end;

            key = new OffsetWrapperWritable<K>(path, start,this.k);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (!more) {
                return false;
            }
            long pos = in.getPosition();

            more = in.next(k, value);
            if (!more || (pos >= end && in.syncSeen())) {
                key = null;
                value = null;
                more = false;
            } else {
                key.setOffset(pos);
            }
            return more;
        }

        @Override
        public OffsetWrapperWritable<K> getCurrentKey() {
            return key;
        }

        @Override
        public V getCurrentValue() {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            if (end == start) {
                return 0.0f;
            } else {
                return Math.min(1.0f, (in.getPosition() - start) / (float)(end - start));
            }
        }

        @Override
        public void close() throws IOException {
            in.close();
        }

    }

    @Override
    public RecordReader<OffsetWrapperWritable<K>, V> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new KeyedOffsetRecordReader<K,V>();
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        return new SequenceFileInputFormat<OffsetWrapperWritable<K>, V>().getSplits(context);
    }
}