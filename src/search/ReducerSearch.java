package search;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerSearch extends Reducer<Text, Text, Text, IntWritable>{

	private static String inputSearchWord = "";
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		
		inputSearchWord = context.getConfiguration().get(ConfigSearch.KEY_WORD);
	}
	
	public void reduce(Text keyIn, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Text val : values) {
			String localText = val.toString();
			if (localText.contains(inputSearchWord)) {
				String[] parts = localText.split(" :: ");
				context.write(keyIn, new IntWritable(Integer.parseInt(parts[0])));
			}
		}
	}
	
}
