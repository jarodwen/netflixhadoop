package csg339.mapreduce.loader;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import java.util.Random;

public class LoadMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {

	public void map(LongWritable key, Text value,
			OutputCollector<LongWritable, Text> output, Reporter reporter)
			throws IOException {
		String line[] = value.toString().split(",");
		int movid = Integer.parseInt(line[0]);
		int usrid = Integer.parseInt(line[1]);
		long seed = usrid * 100000 + movid;
		Random generator = new Random(seed);
		LongWritable newkey = new LongWritable(generator.nextLong());
		//Main.a = Main.a + 1;
		//value.set(Main.a.toString());
		output.collect(newkey, value);
	}
}
