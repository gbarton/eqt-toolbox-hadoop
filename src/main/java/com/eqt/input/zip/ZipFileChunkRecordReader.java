package com.eqt.input.zip;

import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import static com.eqt.input.zip.ZipFileChunkInputFormat.CRASH_ON_ERROR;

/**
 * Opens a zip file, reads each file out and chunks the content into 32KB pieces.
 * Key is a filename:offset string, value is a text with said chunk of data.
 */
public class ZipFileChunkRecordReader extends RecordReader<Text, Text> {
	
	//set our chunk size
	//TODO: needs configuring via conf
	private static int chunkSize = 32 * 1024;
	
	private FSDataInputStream in;
	private ZipInputStream zip;
	private String currKeyPrefix = null;
	private Text currentKey = new Text();
	private Text currentValue = new Text();
	private ZipEntry currEntry = null;
	private boolean isFinished = false;
	
	private long totalBytesRead = 0;
	private long origFileSize = 0;
	
	private long pos = 0;
	private byte[] dataBytes = new byte[chunkSize];
	private int nread = 0;
	
	private Counter uniqueFilesCounter = null;
	private Counter bytesReadCounter = null;
	private Counter corruptZipCounter = null;

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException,
			InterruptedException {
		FileSplit split = (FileSplit) inputSplit;
		Configuration conf = context.getConfiguration();
		Path path = split.getPath();
		FileSystem fs = path.getFileSystem(conf);
		origFileSize = fs.getFileStatus(path).getLen();

		in = fs.open(path);
		zip = new ZipInputStream(in);
		
		//counters
		uniqueFilesCounter = context.getCounter("zipStats","Unique Files Found");
		bytesReadCounter = context.getCounter("zipStats","Uncompressed Bytes");
		corruptZipCounter = context.getCounter("zipStats","Corrupted Zip files.");
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		
		//this will let us spin past the possibility of having pulled exactly chunkSize bits
		//last time from a file that was exactly chunkSize long.
		while(true) {
			//is entry empty? go get one
			if(currEntry == null) {
				try {
					currEntry = zip.getNextEntry();
					if(currEntry != null) {
						uniqueFilesCounter.increment(1);
						//reset key, pos
						currKeyPrefix = currEntry.getName()+":";
						pos = 0;
					}
				} catch(ZipException e) {
					if(CRASH_ON_ERROR)
						throw new IOException(e);
					corruptZipCounter.increment(1);
					return false;
				}
			}
			
			//all done, end of zip?
			if(currEntry == null) {
				isFinished = true;
				return false;
			}
			
			try {
				int curr = 0;
				boolean eof = false;
				while(curr < chunkSize) {
					//suck more data,... slurp!
					nread = zip.read(dataBytes,curr,chunkSize-curr);
					if(nread == -1) {
						eof = true;
						break;
					}
					curr+=nread;
				}
				
				if(curr > 0) {
					currentKey.set(currKeyPrefix + pos);
					currentValue.set(dataBytes,0,curr);
					pos += curr;
					totalBytesRead += curr;
					bytesReadCounter.increment(curr);
				}
				
				if(eof) {
					zip.closeEntry();
					currEntry = null;
					if(curr <= 0)
						continue;
				}
				return true;
			} catch(ZipException e) {
				if(CRASH_ON_ERROR)
					throw new IOException(e);
				corruptZipCounter.increment(1);
				return false;
			}
			
		}
	}
	
	@Override
	public float getProgress() throws IOException, InterruptedException {
		if(totalBytesRead == 0)
			return 0;
		if(isFinished)
			return 1;
		
		//going to assume roughly 7:1 compressed ratio, we just really want to see movement more than anything.
		float val = ((totalBytesRead+0.0f)/(origFileSize*7.0f));
		//hate going over 1
		if(val > 1.0f)
			val = 0.97f;
		return val;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return currentKey;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return currentValue;
	}

	@Override
	public void close() throws IOException {
		try {
			zip.close();
			in.close();
		} catch (Exception ignored) { }
	}
}