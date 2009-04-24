package csg339.mapreduce.loader;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class LoadReducer extends MapReduceBase implements
		Reducer<LongWritable, Text, DoubleWritable, Text> {

	/**
	 * The reducer of the randomizer, which outputs the lines ordered by
	 * the key generated from the mapper. Since the key is randomly generated,
	 * for the possible situation that some lines may have the same key, we
	 * simply output them in order.
	 */
	public void reduce(LongWritable key, Iterator<Text> values,
			OutputCollector<DoubleWritable, Text> output, Reporter reporter)
			throws IOException {
		int counter = 1;
		while(values.hasNext()){
			counter ++;
			output.collect(new DoubleWritable(Double.valueOf(key.toString()) + 1 / counter), new Text(values.next().toString()));
		}
	}

}
