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
//import org.apache.hadoop.mapred.TextOutputFormat;

public class Main {
	
	static Integer i = 0;

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		JobConf conf;

		for (i = 0; i < 2; i++) {

			conf = new JobConf(Main.class);
			conf.setJobName("RatingLoader");

			conf.setOutputKeyClass(LongWritable.class);
			conf.setOutputValueClass(Text.class);

			conf.setMapperClass(LoadMapper.class);
			// conf.setCombinerClass(LoadReducer.class);
			conf.setReducerClass(LoadReducer.class);

			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(RatingOutputFormat.class);

			FileInputFormat.setInputPaths(conf, new Path(
					"/user/jarod/input/small"));

			Path outPath = new Path("/user/jarod/output/small_" + String.valueOf(i));
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

}
