package com.bennight.OSMPBFHadoop;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.openstreetmap.osmosis.osmbinary.BinaryParser;
import org.openstreetmap.osmosis.osmbinary.Osmformat.DenseNodes;
import org.openstreetmap.osmosis.osmbinary.Osmformat.HeaderBlock;
import org.openstreetmap.osmosis.osmbinary.Osmformat.Node;
import org.openstreetmap.osmosis.osmbinary.Osmformat.Relation;
import org.openstreetmap.osmosis.osmbinary.Osmformat.Way;
import org.openstreetmap.osmosis.osmbinary.file.BlockInputStream;
import org.openstreetmap.osmosis.osmbinary.file.BlockReaderAdapter;


public class OSMPBFMapper extends Mapper<LongWritable, BytesWritable, NullWritable, NullWritable> {
	
	private static final Logger sLogger = Logger.getLogger(OSMPBFMapper.class);
	
	@Override
	public void map(LongWritable key, BytesWritable value, Context context) throws IOException, InterruptedException{
		ByteArrayInputStream in = new ByteArrayInputStream(Arrays.copyOfRange(value.getBytes(),0, value.getLength()));
		BlockReaderAdapter brad = new BinaryMapperParser();
		BlockInputStream bis = new BlockInputStream(in, brad);
		bis.process();
	    bis.close();
	    in.close();
	}
	
	 private static class BinaryMapperParser extends BinaryParser {

	        @Override
	        protected void parseRelations(List<Relation> rels) {
	        	//insert relations
	        }

	        @Override
	        protected void parseDense(DenseNodes nodes) {
	        	//insert nodes
	        }

	        @Override
	        protected void parseNodes(List<Node> nodes) {
	            //insert nodes
	        }

	        @Override
	        protected void parseWays(List<Way> ways) {
	            //insert ways
	        }

	        @Override
	        protected void parse(HeaderBlock header) {
	            //no idea
	        }

	        @Override
			public void complete() {
	            sLogger.info("Mapper Completed");
	        }

	    }


}
