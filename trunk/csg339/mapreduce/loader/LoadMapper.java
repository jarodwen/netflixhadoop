package csg339.mapreduce.loader;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import csg339.mapreduce.predlearner.util.Globals;

import java.util.Random;

public class LoadMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {

	/**
	 * Mapper of the randomizer, which generates a random key for each given line
	 * from the input file.
	 * */
	public void map(LongWritable key, Text value,
			OutputCollector<LongWritable, Text> output, Reporter reporter)
			throws IOException {
		String line[] = value.toString().split(",");
		if(line.length > 3){
			throw new IOException("More than three tokens are detected in one line!");
		}
		int movid = Integer.parseInt(line[1]);
		if(Globals.IS_LIMITED && movid > Globals.MOVIE_ID_THRESHOLD)
			return;
		int usrid = Integer.parseInt(line[0]);
		if(Globals.IS_LIMITED && usrid > Globals.USER_ID_THRESHOLD)
			return;
		long seed = usrid * 100000 + movid;
		Random generator = new Random(seed);
		LongWritable newkey = new LongWritable(generator.nextLong());
		output.collect(newkey, value);
	}
}
