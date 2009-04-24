package csg339.mapreduce.loader;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

import csg339.mapreduce.predlearner.util.Globals;

//import org.apache.hadoop.mapred.TextOutputFormat;

public class Main {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		JobConf conf;

		conf = new JobConf(Main.class);
		conf.setJobName("RatingLoader");

		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(LoadMapper.class);
		conf.setReducerClass(LoadReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		/* Here we use a customized output format, 
		 * which will not output the key. */
		conf.setOutputFormat(RatingOutputFormat.class);

		FileInputFormat
				.setInputPaths(conf, new Path(Globals.trainingPath));

		Path outPath = new Path(Globals.randomizedPath);
		try {
			if (outPath.getFileSystem(conf).exists(outPath))
				outPath.getFileSystem(conf).delete(outPath, true);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		FileOutputFormat.setOutputPath(conf, outPath);
		JobClient.runJob(conf);
	}

}
