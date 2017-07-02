package top20RatedMovieName;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Top20RatedMovieNameMapper extends Mapper<LongWritable, Text, Text, Text>{
		
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException{
		
		String[] strSplit = value.toString().split("::");
		context.write(new Text(strSplit[0]), new Text(strSplit[1]));
	}
	
}
