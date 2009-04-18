package csg339.mapreduce.predlearner.learner;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import csg339.mapreduce.predlearner.util.FeatureID;
import csg339.mapreduce.predlearner.util.FeatureVector;
import csg339.mapreduce.predlearner.util.Globals;



public class LearnJob {

	
	public static void learn(int counter) throws Exception
	{
		JobConf conf = new JobConf(LearnJob.class);
		conf.setJobName("LearnJob");
		conf.setOutputKeyClass(Writable.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(LearnMap.class);
		conf.setMapOutputKeyClass(FeatureID.class);
		conf.setMapOutputValueClass(FeatureVector.class);
		conf.setCombinerClass(LearnCombine.class);
		conf.setReducerClass(LearnReduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.set("learnmap.features.path", "/user/jarod/output_"+ String.valueOf(counter) +"/part-00000");
		//DistributedCache.addCacheFile(new Path("/user/jarod/output_0/part-00000").toUri(), conf);
		FileInputFormat.setInputPaths(conf, new Path(Globals.trainingPath));
		Path outPath = new Path("/user/jarod/output_" + String.valueOf(counter + 1));
		try {
			if (outPath.getFileSystem(conf).exists(outPath))
				outPath.getFileSystem(conf).delete(outPath, true);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		FileOutputFormat.setOutputPath(conf, outPath);
		try {
			System.out.println("JobNext...");
			JobClient.runJob(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
