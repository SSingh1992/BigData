package top20RatedMovieID;

/*Keeping count is important:
Count required in the split
it helps in setting movies 
watched less than 40. This 
will be done in Partitioner 
Task condition
Input:
MovieID \t Average \t count
Output:
Key>1231
Value>average 4.2331 count 1323*/

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountMovieNameAvgMapper 
extends Mapper<LongWritable, Text, Text, Text>{

	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException{
		
		String[] strSplit = value.toString().split("\t");
		
		context.write(new Text(strSplit[0]), new Text(strSplit[1]));		
	}
}
