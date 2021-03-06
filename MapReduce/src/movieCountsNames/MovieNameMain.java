package movieCountsNames;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

public class MovieNameMain {

	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf = new Configuration();
		Job job = new Job();
		job.setJarByClass(MovieNameMain.class);
		
		job.setReducerClass(MovieNameReduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieNameMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MovieWatchCountMapper.class);
		Path outputPath = new Path(args[2]);
		
		//args[0] - hdfs://quickstart.cloudera:8020/user/data/ml-1m/movies.dat
		//args[1] - hdfs://quickstart.cloudera:8020/user/output/part-r-00000
		//args[2] - hdfs://quickstart.cloudera:8020/user/output/name
		
		/*
		hdfs://quickstart.cloudera:8020/user/data/ml-1m/movies.dat hdfs://quickstart.cloudera:8020/user/output/part-r-00000 hdfs://quickstart.cloudera:8020/user/output/name
		*/
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		FileOutputFormat.setOutputPath(job, outputPath);
		job.waitForCompletion(true);
		
	}
}
