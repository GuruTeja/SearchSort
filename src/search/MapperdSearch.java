package search;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MapperdSearch extends Mapper<Object, Text, Text, Text>{
	
	private Text OutputKey = new Text();
	private String LineName;
	private static int linelocation = 0;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		LineName = ((FileSplit) context.getInputSplit()).getPath()
				.toString();
	}
	
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		linelocation++;
		while (itr.hasMoreTokens()) {
			OutputKey.set(LineName);
			context.write(OutputKey, new Text(linelocation + " :: " + itr.nextToken()));
		}
	}
	

}
