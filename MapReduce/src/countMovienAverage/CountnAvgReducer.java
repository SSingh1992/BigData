package countMovienAverage;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountnAvgReducer 
extends Reducer<Text, Text, Text, Text>{
	
	
	public void reduce(Text key, Iterable <Text> values, Context context) 
			throws IOException, InterruptedException{
		float ratingSum = 0;
		float countSum = 0;
		Text result = new Text();
		
		for (Text val: values){
			String[] str = val.toString().split("\t");
			//value --> rating
			ratingSum += Integer.parseInt(str[0]);
			countSum += Integer.parseInt(str[1]);
		}
		
		String avg = Float.toString((ratingSum/countSum));
		String count = Float.toString(countSum);
		result.set(avg+"\t"+count);
		
		if(countSum > 40){
			context.write(key, result);
		}
		
	}

}
