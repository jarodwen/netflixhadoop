package csg339.mapreduce.loader;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class LoadReducer extends MapReduceBase implements
		Reducer<LongWritable, Text, LongWritable, Text> {

	public void reduce(LongWritable key, Iterator<Text> values,
			OutputCollector<LongWritable, Text> output, Reporter reporter)
			throws IOException {
		String newValue = "";
		while(values.hasNext()){
			Integer tempInt = Integer.valueOf(values.next().toString());
			//Main.a = Main.a + 1;
			//newValue += String.valueOf(Main.a);
		}
		output.collect(key, new Text(newValue));
	}

}
