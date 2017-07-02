package movieCountsNames;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieNameReduce 
extends Reducer <Text, Text, Text, Text>{
	
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException{
		
		StringBuilder strBuilder = new StringBuilder();
		String str_movieNcount = null;
		String name = "";
		String count = "";
			
		for (Text t : values){			
			String strSplit[] = t.toString().split(" ");
			if(strSplit[0].equals("name")){
				//name = strSplit[1];
				for(int i = 1; i < strSplit.length; i++){
					strBuilder.append(strSplit[i]);
					strBuilder.append(" ");
				}
				name = strBuilder.toString();
			}else if(strSplit[0].equals("count")){
				count = strSplit[1];
			}			
		}
		str_movieNcount = ""+name+"\t"+count;
		context.write(key, new Text(str_movieNcount));		
	}

}
