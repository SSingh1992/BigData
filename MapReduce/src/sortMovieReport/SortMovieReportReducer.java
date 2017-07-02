package sortMovieReport;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class SortMovieReportReducer
extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
	
	private static final TreeMap<Integer, Integer> TopKMap = new TreeMap<>();	
	
	@Override
	public void reduce(IntWritable key, Iterable<IntWritable> valueList, Context context) 
			throws IOException, InterruptedException{			
		int valueInt = 0;		
		for (IntWritable value : valueList){			
			valueInt = value.get();
		}			
		if (TopKMap.size() <= 20)
		{
			TopKMap.put(valueInt, key.get());
		}
		else if (TopKMap.firstKey() < valueInt) {
			TopKMap.pollFirstEntry();
			TopKMap.put(valueInt, key.get());			
		}
	}	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException{
		
		for (Integer key : TopKMap.keySet()){
			context.write(new IntWritable (key), new IntWritable(TopKMap.get(key)));
		}		
	}
}
