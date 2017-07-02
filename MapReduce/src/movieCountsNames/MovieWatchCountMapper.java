package movieCountsNames;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieWatchCountMapper 
extends Mapper<LongWritable, Text, Text, Text>{
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException{
		
		String[] strSplit = value.toString().split("\t");
		context.write(new Text(strSplit[0]), new Text("count "+strSplit[1]));
		
		// strSplit[0] -- movieId  ,  strSplit[1] -- movieCount
		//movieId - key
		// Movie count data is stored at hdfs://quickstart.cloudera:8020/user/output/part-r-00000
		
	}

}
