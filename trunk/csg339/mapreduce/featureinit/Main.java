package csg339.mapreduce.featureinit;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

//import org.apache.hadoop.mapred.TextOutputFormat;

public class Main {

	static Integer i = 0;

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		JobConf conf;

		conf = new JobConf(Main.class);
		conf.setJobName("FeatureInitializer");

		/*
		 * Set the out put key-value pair: 
		 * key: {(int)id, (byte)um_type} 
		 * value: feature_1 feature_2 ... feature_MAX 
		 * */
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(FeatureInitMapper.class);
		conf.setReducerClass(FeatureInitReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat
				.setInputPaths(conf, new Path("/user/hadoop/projectJJ/small_input"));

		Path outPath = new Path("/user/hadoop/projectJJ/small_input_feat");

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
