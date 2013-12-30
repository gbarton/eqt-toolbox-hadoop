package com.eqt.input.zip;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class ZipFileChunkInputFormat extends FileInputFormat<Text,Text> {

	//Should we fail the task if the zip file has issues?
	//TODO: make configurable via conf
	public static boolean CRASH_ON_ERROR = false;
	
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}
	
	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new ZipFileChunkRecordReader();
	}
}
