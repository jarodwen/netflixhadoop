package csg339.mapreduce.predictor.newtest;

import java.io.IOException;
import java.util.Iterator;

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

public class Main {
	
	public static Features features = new Features();
	
	static final int MAX_ITERATIONS = 2;

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		/* MapReduce configuration part */
		JobConf conf = new JobConf(Main.class);

		for (int i = 0; i < MAX_ITERATIONS; i++) {
			conf.setJobName("RatingPred");

			conf.setOutputKeyClass(LongWritable.class);
			conf.setOutputValueClass(Text.class);

			conf.setMapperClass(PredMapper.class);
			// conf.setCombinerClass(LoadReducer.class);
			conf.setReducerClass(PredReducer.class);

			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);

			FileInputFormat.setInputPaths(conf, new Path(
					"/user/jarod/input/small"));

			Path outPath = new Path("/user/jarod/output/small_s"
					+ String.valueOf(i));
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

		/* Write output */
		System.out.print(features.toString());
	}

	
	public class PredMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		double K = 0.015;   // Regularization parameter used to minimize over-fitting
		double LRATE = 0.001; // Learning rate parameter
		
		public PredMapper(){
			
		}
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter repoter) throws IOException {
			String rate = value.toString().trim();
			if(rate == "")
				return;
			String elements[] = rate.split(",");
			if(elements.length != 3)
				return;
			//System.out.println(rate);
			//System.out.print(elements[0] + " " + elements[1] + " " + elements[2]+"\n");
			long user_id = Long.valueOf(elements[0]);
			int movie_id = Integer.valueOf(elements[1]);
			short rating = Short.valueOf(elements[2]);
			
			String twoValues = getMappedValue(user_id, movie_id, rating);
			String values[] = twoValues.split("|");
			
			output.collect(new Text("u" + String.valueOf(user_id)), new Text(values[0]));
			output.collect(new Text("m" + String.valueOf(movie_id)), new Text(values[1]));
		}
		
		private String getMappedValue(long user_id, int movie_id, short rating){
		
			String movie_features = "";
			String user_features = "";
			
			double cf, mf;
			
			double p = predictRating(user_id, movie_id);
			double err = rating-p;
			
			for(int index = 0;index < Features.MAX_FEATURES; index++)
			{
				err = rating-p;

				cf = Main.features.getUserFeature(user_id, index);
				mf = Main.features.getMovieFeature(movie_id, index);
				
				// Cross-train the features
				double user_feature = cf + LRATE *( err * mf - K * cf);
				double movie_feature = mf + LRATE *( err * cf - K * mf);
				
				//ratingSet.features.get(movieID).set(f,Globals.LRATE *( err * cf - Globals.K * mf));
				//ratingInput.mfv.set(f,Globals.LRATE *( err * cf - Globals.K * mf));
				//              p = p - (cf * mf) + (userFeatureVector[f]*(movieFeatures[movieId][f]));
				//p = p - (cf* mf) + ratingSet.features.get(userID).get(f) * ratingSet.features.get(movieID).get(f);
				p = p - (user_feature * movie_feature) ;
				if(p > 5)
					p = 5;
				else if (p < 1)
					p = 1;
				user_features += String.valueOf(user_feature) + ",";
				movie_features += String.valueOf(movie_feature) + ",";
			}
			
			
			return user_features.substring(0, user_features.length() - 2).trim() 
					+ "|" 
					+ movie_features.substring(0, movie_features.length() - 2).trim();
		}
		
		private double predictRating(long user_id, int movie_id){
			double returnPrediction = 1;

			//Baseline Considerations:
			//average + baselineMovie[movieId] + baselineUser[userId];

			for(int index = 0;index < Features.MAX_FEATURES;index++)
			{
				returnPrediction += Main.features.getUserFeature(user_id, index)
									* Main.features.getMovieFeature(movie_id, index);
				
			}
			if(returnPrediction > 5) returnPrediction = 5;
			if(returnPrediction < 1) returnPrediction = 1;
			return returnPrediction;
		}

	}

	public class PredReducer extends MapReduceBase implements Reducer<Text, Text, LongWritable, Text> {

		public PredReducer(){
			
		}
		
		public void reduce(Text _key, Iterator<Text> values,
				OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			while (values.hasNext()) {
				String value = values.next().toString();
				String id = _key.toString();
				if(id.charAt(0) == 'u'){
					String feat_strs[] = value.split(",");
					long user_id = Long.valueOf(id.substring(1));
					for(int i = 0; i < feat_strs.length; i++){
						features.setUserFeature(user_id, i, Double.valueOf(feat_strs[i]));
					}
				}else{
					String feat_strs[] = value.split(",");
					int movie_id = Integer.valueOf(id.substring(1));
					for(int i = 0; i < feat_strs.length; i++){
						features.setMovieFeature(movie_id, i, Double.valueOf(feat_strs[i]));
					}
				}
				// process value
			}
		}
	}

}
