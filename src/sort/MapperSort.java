package sort;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperSort extends Mapper<Object, Text, Text, IntWritable>{
	
	private Text keysOut = new Text();
	private IntWritable valsOut = new IntWritable();
	
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		
		StringTokenizer strTokenize = new StringTokenizer(value.toString());
		
		while (strTokenize.hasMoreElements()) {
			 
			int number = Integer.parseInt(strTokenize.nextToken());
			
			keysOut.set(number + "");
			
			valsOut.set(number);
			
			context.write(keysOut, valsOut);
			
		}
	}
	
	

}
