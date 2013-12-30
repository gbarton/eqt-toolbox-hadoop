package com.eqt.input.sequence.offset;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class OffsetWrapperWritable<K extends WritableComparable> implements WritableComparable<OffsetWrapperWritable> {

    private Text t = new Text();
    private Path path;
    private long offset;
    private K key;

    public OffsetWrapperWritable(Path path, long offset, K Key) {
        this.path = path;
        this.offset = offset;
    }

    public K getKey() {
    	return key;
    }
    
    public void setKey(K key) {
    	this.key = key;
    }
    
    public Path getPath() {
        return path;
    }

    public long getOffset() {
        return offset;
    }

    public void setPath(Path path) {
        this.path = path;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        t.readFields(in);
        path = new Path(t.toString());
        offset = in.readLong();
        key.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        t.set(path.toString());
        t.write(out);
        out.writeLong(offset);
        key.write(out);
    }

    @SuppressWarnings("unchecked")
	@Override
    public int compareTo(OffsetWrapperWritable o) {
    	int i = key.compareTo(o.key);
    	if(i != 0)
    		return i;
    	
        int x = path.compareTo(o.path);
        if (x != 0) {
            return x;
        } else {
            return Long.valueOf(offset).compareTo(Long.valueOf(o.offset));
        }
    }
}