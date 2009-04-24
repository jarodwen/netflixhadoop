package csg339.mapreduce.predlearner.learner;

import java.io.IOException;

//import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

//import csg339.mapreduce.predlearner.util.FeatureID;
import csg339.mapreduce.predlearner.util.FeatureVector;
import csg339.mapreduce.predlearner.util.Globals;

/**
 * This class contains the job configuration for the map/reduce
 * interative SVD algorithm. 
 * 
 * @author jake & jarod
 *
 */
public class LearnJob {

	
	public static void learn(int counter) throws Exception
	{
		JobConf conf = new JobConf(LearnJob.class);
		conf.setJobName("LearnJob");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(LearnMap.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(FeatureVector.class);
		
		conf.setCombinerClass(LearnCombine.class);
		
		conf.setReducerClass(LearnReduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		/* Set the initial feature vector file. Each phrase this will be adjusted
		 * to the feature vector file generated in the previous phrase.
		 */
		conf.set("learnmap.features.path", Globals.featPathPrefix + String.valueOf(counter) +"/part-00000");
		
		/* ToDo: Distributed Cache may be used here as an alternative for the 
		 * configuration files. 
		 */
		//if(Globals.IS_DEBUG) System.out.println(Globals.featPathPrefix + String.valueOf(counter) + "/part-00000");
		//DistributedCache.addCacheFile(new Path(Globals.featPathPrefix + String.valueOf(counter) + "/part-00000").toUri(), conf);
		
		FileInputFormat.setInputPaths(conf, new Path(Globals.randomizedPath));
		Path outPath = new Path(Globals.featPathPrefix + String.valueOf(counter + 1));
		try {
			if (outPath.getFileSystem(conf).exists(outPath))
				outPath.getFileSystem(conf).delete(outPath, true);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		FileOutputFormat.setOutputPath(conf, outPath);
		try {
			System.out.println("Job Phrase "+String.valueOf(counter)+"...");
			JobClient.runJob(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
