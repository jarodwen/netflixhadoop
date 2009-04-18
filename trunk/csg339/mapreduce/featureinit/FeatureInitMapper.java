package csg339.mapreduce.featureinit;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class FeatureInitMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, NullWritable> {
	
	public void map(LongWritable key, Text value,
			OutputCollector<Text, NullWritable> output, Reporter reporter)
			throws IOException {
		String line[] = value.toString().split(",");
		int movid = Integer.parseInt(line[0]);
		int usrid = Integer.parseInt(line[1]);
		
		output.collect(new Text(String.valueOf(usrid)+"\t"+"1"), null);
		output.collect(new Text(String.valueOf(movid)+"\t"+"0"), null);
	}
}
