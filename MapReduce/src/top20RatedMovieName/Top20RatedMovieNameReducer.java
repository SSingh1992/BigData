package top20RatedMovieName;

import java.io.BufferedReader;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class Top20RatedMovieNameReducer extends Reducer<Text, Text, Text, Text>{
	
	private BufferedReader brReader;

	@Override
	public void setup(Context context) throws IOException{
		Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		for (Path eachPath : cacheFilesLocal){
			if(eachPath.getName().toString().trim().equals("hdfs://quickstart.cloudera:8020/user/output/nameAvgCount/part-r-00000")){
				top20RatedMovieIdSplit(eachPath, context);
			}
		}		
	}

	private void top20RatedMovieIdSplit(Path eachPath, Context context) throws IOException {
		FileSystem dfs = FileSystem.get(context.getConfiguration());
		 
	}

	public void reduce(Text key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException{
		
		context.write(key, (Text) value);
	}
}
