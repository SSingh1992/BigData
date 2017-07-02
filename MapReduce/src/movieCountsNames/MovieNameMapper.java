package movieCountsNames;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieNameMapper 
extends Mapper<LongWritable, Text, Text, Text>{
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException{
		
		String[] strSplit = value.toString().split("::");
		context.write(new Text(strSplit[0]), new Text("name "+strSplit[1]));
		
		
		//movies.dat -- stored at location -- /user/data/ml-1m/movies.dat
		//Format -- moviesId::MovieName::MovieCategory
		
	}

}
