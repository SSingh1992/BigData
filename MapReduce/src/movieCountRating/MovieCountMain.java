package movieCountRating;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MovieCountMain {
	
	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf = new Configuration();
		Job job = new Job();
		job.setJarByClass(MovieCountMain.class);
		
		job.setMapperClass(MovieRatingCountMapper.class);
		job.setReducerClass(MovieCountReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setMapOutputKeyClass(Text.class);		
		job.setMapOutputValueClass(LongWritable.class);		
		
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		Path out = new Path(args[0]);
		out.getFileSystem(conf).delete(out, true);	   
		
		FileInputFormat.addInputPaths(job, "hdfs://quickstart.cloudera:8020/user/data/ml-1m/ratings.dat");
	    FileOutputFormat.setOutputPath(job, new Path(args[0]));
		
	    job.waitForCompletion(true);
	}
}
