package csg339.mapreduce.loader;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import java.util.Iterator;
import java.util.Random;

public class RatingLoader {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf();
		conf.setJobName("RatingLoader");
		
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(LoadMapper.class);
		conf.setCombinerClass(LoadReducer.class);
		conf.setReducerClass(LoadReducer.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
	}

	public static class LoadMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {

		public void map(LongWritable key, Text value,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {
			String line[] = value.toString().split(" ");
			int movid = Integer.parseInt(line[0]);
			int usrid = Integer.parseInt(line[1]);
			long seed = usrid * 100000 + movid;
			Random generator = new Random(seed);
			LongWritable newkey = new LongWritable(generator.nextLong());
			output.collect(newkey, value);
		}

		@Override
		public void configure(JobConf arg0) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			
		}

	}

	public static class LoadReducer extends MapReduceBase implements
			Reducer<LongWritable, Text, LongWritable, Text> {

		public void reduce(LongWritable key, Iterator<Text> values,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {
			String newValue = "";
			while(values.hasNext()){
				newValue += values.toString();
				newValue += "\n";
			}
			output.collect(key, new Text(newValue));
		}

	}

}
