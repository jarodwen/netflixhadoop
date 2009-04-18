package csg339.mapreduce.featureinit;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class FeatureInitReducer extends MapReduceBase implements
		Reducer<Text, NullWritable, Text, Text> {

	static int MAX_FEATURES = 4;

	public void reduce(Text key, Iterator<NullWritable> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		/*
		 * For all the possible values in one key, we only output one feature
		 * vector for it. All the others will be omitted.
		 */
		String featVecStr = "";
		for (int i = 0; i < MAX_FEATURES; i++) {
			if (i != 0)
				featVecStr += "\t";
			featVecStr += "0.1";
		}
		output.collect(key, new Text(featVecStr));
	}

}
