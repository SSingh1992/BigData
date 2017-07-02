package top20RatedMovieID;

//Gives TOP 20 xx rated moviesID in the dataset

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import movieCountsNames.MovieNameMapper;

public class CountMovieNameAvgMain {
	
	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException{
				
		Configuration conf = new Configuration();
		Job job = new Job();
		job.setJarByClass(MovieNameMapper.class);
		
		job.setReducerClass(CountMovieNameAvgReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(1);
		
		//MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieNameMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, CountMovieNameAvgMapper.class);
		Path outputPath = new Path(args[2]);
		
		//args[0] - hdfs://quickstart.cloudera:8020/user/data/ml-1m/movies.dat
		//args[1] - hdfs://quickstart.cloudera:8020/user/output/part-r-00000
		//args[2] - hdfs://quickstart.cloudera:8020/user/output
		
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		FileOutputFormat.setOutputPath(job, outputPath);
		job.waitForCompletion(true);
		
	}
}
