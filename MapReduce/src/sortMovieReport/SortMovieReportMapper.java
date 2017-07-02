package sortMovieReport;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMovieReportMapper 
extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
	
	private static int N =10;
	
	private TreeMap<IntWritable, IntWritable> TopKMap = new TreeMap<IntWritable, IntWritable>();
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException{
		
		String[] strSplit = value.toString().split("\t");
		
		int keyMap = Integer.parseInt(strSplit[0]);
		int valueMap = Integer.parseInt(strSplit[1]);
		
	/*	TopKMap.put(new IntWritable(keyMap), new IntWritable(valueMap));
		
		if (TopKMap.size() > N){
			TopKMap.remove(TopKMap.firstEntry());
		}*/
		
		
		context.write(new IntWritable(valueMap), new IntWritable(keyMap));
			
	}
	
	/*protected void cleanup(Context context) 
			throws IOException, InterruptedException{
		for(IntWritable key : TopKMap.keySet()){
			context.write(key, TopKMap.get(key));
		}
	}*/
}
