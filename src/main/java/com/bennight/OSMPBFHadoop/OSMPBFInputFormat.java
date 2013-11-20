package com.bennight.OSMPBFHadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;
import org.openstreetmap.osmosis.osmbinary.Fileformat.BlobHeader;



public class OSMPBFInputFormat extends FileInputFormat<LongWritable,BytesWritable> {

private static final Logger sLogger = Logger.getLogger(OSMPBFInputFormat.class);
private static final int DATABLOCKS_PER_MAPPER = 10;

@Override
public List<InputSplit> getSplits(JobContext context){
	List<InputSplit> splits = new ArrayList<InputSplit>();
	FileSystem fs = null;
    Path file = FileInputFormat.getInputPaths(context)[0];
    FSDataInputStream in = null;
    try {  //File spec @ http://wiki.openstreetmap.org/wiki/PBF_Format
		fs = FileSystem.get(context.getConfiguration());
		in = fs.open(file);
	    long splitStart = 0;
	    long splitLength = 0;
	    while (in.available() > 0){
	    	splitLength = 0;
	    	for (int dataBlock = 0; dataBlock < DATABLOCKS_PER_MAPPER; dataBlock++){
	    		if (in.available() == 0) {break;}
		    	int len = in.readInt(); 
		    	byte[] blobHeader = new byte[len]; 
		    	in.read(blobHeader);
		    	BlobHeader h = BlobHeader.parseFrom(blobHeader);
		    	splitLength += 4;
		    	splitLength += len;
		    	splitLength += h.getDatasize();
		    	in.skip(h.getDatasize());
	    	}
	    	FileSplit split = new FileSplit(file, splitStart,splitLength, new String[] {});
	    	splits.add(split);
	    	splitStart += splitLength;
	    }
	} catch (IOException e) {
		sLogger.error(e.getLocalizedMessage());
	} finally {
		if (in != null) {try {in.close();}catch(Exception e){}};
	}
	return splits;
}
	
	
	@Override
	public OSMPBFRecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		return new OSMPBFRecordReader();
	}
	
	@Override
	protected long getFormatMinSplitSize(){
		return 1l;
	}
	
}
