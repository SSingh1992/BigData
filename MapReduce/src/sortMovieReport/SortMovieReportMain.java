package sortMovieReport;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SortMovieReportMain {
	
	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf = new Configuration();
		Job job = new Job();
		
		job.setJarByClass(SortMovieReportMain.class);
		
		job.setMapperClass(SortMovieReportMapper.class);
		job.setReducerClass(SortMovieReportReducer.class);
		job.setNumReduceTasks(1);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setSortComparatorClass(IntComparator.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		Path out = new Path(args[0]);
		out.getFileSystem(conf).delete(out, true);	   
		
		FileInputFormat.addInputPaths(job, "hdfs://quickstart.cloudera:8020/user/output/part-r-00000");
	    FileOutputFormat.setOutputPath(job, new Path(args[0]));
		
	    job.waitForCompletion(true);		
		
	}

}
