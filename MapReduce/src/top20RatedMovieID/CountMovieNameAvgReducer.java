package top20RatedMovieID;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CountMovieNameAvgReducer extends Reducer<Text, Text, Text, Text>{
	
	private static final TreeMap<Float, Integer> TopKMap = new TreeMap<>();

	public void reduce(Text key, Iterable<Text> value, Context context) 
			throws IOException, InterruptedException{
		
				/*StringBuilder strBuilder = new StringBuilder();
				String movieName = "";
				String avgNcount = "";
				String movieName_avgNcount = "";
				String countNo = "";
			 	String[] strSplit = t.toString().split(" ");
			if(strSplit[0].equals("name")){
				for(int i = 1; i < strSplit.length; i++){
					strBuilder.append(strSplit[i]);
					strBuilder.append(" ");
				}
				movieName = strBuilder.toString();
			}else if(strSplit[0].equals("average")){				
					avgNcount = strSplit[1]+"\t"+strSplit[3];
					countNo = strSplit[3];
			}
		}*/
		
		float valueFloat = 0;
		for(Text t : value){						
				valueFloat = Float.parseFloat(t.toString());
			}	
		
			if (TopKMap.size() <= 20)
			{
				TopKMap.put(valueFloat, Integer.parseInt(key.toString()));
			}
			else if (TopKMap.firstKey() < valueFloat) {
				TopKMap.pollFirstEntry();
				TopKMap.put(valueFloat, Integer.parseInt(key.toString()));
			}
				
	}
	
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException{
		NavigableMap<Float, Integer> nmap = TopKMap.descendingMap();
		
		for (Float key : nmap.keySet()){
			context.write(new Text (key.toString()), new Text(nmap.get(key).toString()));
		}		
	}
}
