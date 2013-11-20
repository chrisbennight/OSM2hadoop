package com.bennight.OSMPBFHadoop;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;
import org.openstreetmap.osmosis.osmbinary.Fileformat.BlobHeader;




public class OSMPBFRecordReader extends RecordReader<LongWritable,BytesWritable> {
	
	private static final Logger sLogger = Logger.getLogger(OSMPBFRecordReader.class);
	
	  private long start;
	  private long end;
	  private long pos;
	  
	  private FSDataInputStream in;
	  private LongWritable key = null;
	  private BytesWritable value = null;
	  
	  private Long uuid = UUID.randomUUID().getMostSignificantBits();  
	  
	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context)throws IOException, InterruptedException {
		FileSplit split = (FileSplit) inputSplit;
		Path file = split.getPath();
		FileSystem fs = file.getFileSystem(context.getConfiguration());
		in = fs.open(file);
		start = pos = split.getStart();
		end = start + split.getLength();
		in.seek(start);
	}  
	
	@Override
	public void close() throws IOException {
		in.close();
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,	InterruptedException {
		return key;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		  if (start == end) {
	            return 0.0f;
	        } else {
	            return Math.min(1.0f, (pos - start) / (float) (end - start));
	        }
	}

	

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (in.available() == 0) return false;
        if (pos >= end) return false;
		if (key == null) {
	            key = new LongWritable();
	        }
	        key.set(pos);
	        
	        if (value == null) {
	            value = new BytesWritable();
	        }
	        in.seek(pos);
	    	int len = in.readInt();
	    	byte[] blobHeader = new byte[len];
	    	in.read(blobHeader);
	    	BlobHeader h = BlobHeader.parseFrom(blobHeader);
	    	byte[] blob = new byte[h.getDatasize() + len + 4];
	    	in.seek(pos);
	    	in.read(blob);
	    	pos += 4;
	    	pos += len;
	    	pos += h.getDatasize();
	    	value.set(blob, 0, blob.length);
	        return true;
	}

	
}
