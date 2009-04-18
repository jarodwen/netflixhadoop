package csg339.mapreduce.predictor.dbinterface;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class PredReducer extends MapReduceBase implements Reducer<Text, Text, Text, FeaturesDBWritable> {
	
	public void reduce(Text _key, Iterator<Text> values,
			OutputCollector<Text, FeaturesDBWritable> output, Reporter reporter) throws IOException {
		
		
		double[] new_features_value = new double[FeaturesDBWritable.MAX_FEATURES];
		int counter = 0;
		int id = Integer.valueOf(_key.toString().substring(1));
		boolean um_type = true;
		if(_key.toString().charAt(0) == 'u'){
			um_type = true;
		}else{
			um_type = false;
		}
		System.out.println(_key.toString());
		while (values.hasNext()) {
			// process value
			String features[] = values.next().toString().split(",");
			
			for(int i = 0; i < features.length; i++){
				new_features_value[i] += Double.valueOf(features[i]);
			}
			counter ++;
		}
		String new_features = "";
		for(int i = 0; i < FeaturesDBWritable.MAX_FEATURES; i++){
			new_features_value[i] /= counter;
			new_features += String.valueOf(new_features_value[i]) + ",";
		}
		output.collect(_key, new FeaturesDBWritable(id, um_type, "0, 0, 0"));
	}

}
