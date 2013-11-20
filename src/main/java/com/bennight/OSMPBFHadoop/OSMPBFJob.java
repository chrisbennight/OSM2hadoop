package com.bennight.OSMPBFHadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class OSMPBFJob extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(OSMPBFJob.class);
	
    public static void main(String[] args) throws Exception {
    	  int res = ToolRunner.run(new Configuration(), new OSMPBFJob(), args);
          System.exit(res);
    }

	@Override
	public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = new Job(conf, "OSM PBF Import");
        job.setNumReduceTasks(0);
        job.setJarByClass(OSMPBFJob.class);
        job.setMapperClass(OSMPBFMapper.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("/tmpunused"));
        job.setInputFormatClass(OSMPBFInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        return job.waitForCompletion(true) ? 0 : 1;
	}

	
}
