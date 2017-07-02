package countMovienAverage;

//Gives the complete list of the average rating and movieID

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CountnAvgMain {

	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf = new Configuration();
		Job job = new Job();
		job.setJarByClass(CountnAvgMain.class);
		
		job.setMapperClass(CountnAvgMapper.class);
		job.setReducerClass(CountnAvgReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path out = new Path("hdfs://quickstart.cloudera:8020/user/output/avg_rating40");
		out.getFileSystem(conf).delete(out, true);
		
		FileInputFormat.addInputPaths(job, "hdfs://quickstart.cloudera:8020/user/data/ml-1m/ratings.dat");
		FileOutputFormat.setOutputPath(job, out);
		
		job.waitForCompletion(true);
		
	}
}
