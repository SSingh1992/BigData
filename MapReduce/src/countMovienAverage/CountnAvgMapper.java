package countMovienAverage;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*Key --> movieID
Value --> rating	1*/

public class CountnAvgMapper
extends Mapper<LongWritable, Text, Text, Text>{
	private Text word = new Text();
	private Text count = new Text();
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException{
		
		String[] strSplit = value.toString().split("::");
		word.set(strSplit[1]);
		count.set(strSplit[2]+"\t"+1);
		context.write(word, count);		
	}
}
