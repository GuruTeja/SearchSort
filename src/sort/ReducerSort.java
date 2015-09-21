package sort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerSort extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	private List<Integer> Listoriginal = new ArrayList<Integer>();
	private Text text = new Text();
	
	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> values,
			Context context)
					throws IOException, InterruptedException {
		
		
		for(IntWritable intvalues : values){
			Listoriginal.add(intvalues.get());
		}
	}
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		
		Collections.sort(Listoriginal);
		
		for(Integer aaa: Listoriginal){
			context.write(new Text(""), new IntWritable(aaa));
		}
	}

}
