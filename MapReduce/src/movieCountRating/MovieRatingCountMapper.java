package movieCountRating;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieRatingCountMapper 
extends Mapper<LongWritable, Text, Text, LongWritable>{
	private Text word = new Text();
	private LongWritable count = new LongWritable();
	
	public void map(LongWritable key, Text value, Context context) 
		throws IOException, InterruptedException{
					
		String[] strSplit = value.toString().split("::");
		word.set(strSplit[1]);
		
		if(strSplit.length > 1){
			try{				
				count.set(1);
				context.write(word, count);				
			}catch(Exception ex){
				ex.printStackTrace();
			}
		}
		
	}
}
