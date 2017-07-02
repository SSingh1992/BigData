package top20RatedMovieName;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*Gives Top 20 rated Movie Name.
/user/output/nameAvgCount/part-r-00000 is the DistributedCache File*/

public class Top20RatedMovieNameMain {

	public static void main(String[] args) 
			throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException{
		
		Configuration conf = new Configuration();
		Job job = new Job();
		job.setJarByClass(Top20RatedMovieNameMain.class);
		
		job.setMapperClass(Top20RatedMovieNameMapper.class);
		job.setReducerClass(Top20RatedMovieNameReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		DistributedCache.addCacheArchive(new URI("hdfs://quickstart.cloudera:8020/user/output/nameAvgCount/part-r-00000"), conf);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		Path out = new Path("hdfs://quickstart.cloudera:8020/user/output/top20RatedMovieName");
		out.getFileSystem(conf).delete(out, true);
		
		FileInputFormat.addInputPaths(job, "hdfs://quickstart.cloudera:8020/user/data/ml-1m/movies.dat");
		FileOutputFormat.setOutputPath(job, out);
		
		job.waitForCompletion(true);
	}
}
