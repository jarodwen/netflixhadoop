package csg339.mapreduce.predictor.dbinterface;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PredMapper extends MapReduceBase implements Mapper<LongWritable, RatingDBWritable, Text, Text> {

	public static double K = 0.015;   // Regularization parameter used to minimize over-fitting
	public static double LRATE = 0.001; // Learning rate parameter
	static int MAX_FEATURES = 3;
	
	private static final Log LOG = LogFactory.getLog(PredMapper.class);
	
	public void map(LongWritable key, RatingDBWritable value, OutputCollector<Text, Text> output, Reporter repoter) throws IOException {
		String rate = value.toString().trim();
		System.out.println("LN1");
		if(rate == ""){
			LOG.debug("Rate is empty!\n");
			return;
		}
		System.out.println("LN2");
		String elements[] = rate.split(",");
		if(elements.length != 3){
			LOG.debug("Elements are less than 3!\n");
			return;
		}
		System.out.println("LN3");
		System.out.println(rate);
		//System.out.print(elements[0] + " " + elements[1] + " " + elements[2]+"\n");
		long user_id = value.get_user_id();
		int movie_id = value.get_movie_id();
		short rating = value.get_rating();
		String u_features = value.get_u_features();
		String m_features = value.get_m_features();
		
		String features_return = getMappedValue(user_id, movie_id, rating, u_features, m_features);
		
		String features[] = features_return.split("|");
		if(features.length!=2){
			LOG.debug("Features returned are less than 2!\n");
			return;
		}
		System.out.println("LN4");
		LOG.debug("Get features: "+features_return+"\n");
		output.collect(new Text("u" + String.valueOf(user_id)), new Text(features[0]));
		output.collect(new Text("m" + String.valueOf(movie_id)), new Text(features[1]));
	}
	
	private String getMappedValue(long user_id, int movie_id, short rating, String u_features, String m_features){
	
		double movie_features[] = new double[MAX_FEATURES];
		double user_features[] = new double[MAX_FEATURES];
		
		String user_features_rtn = "";
		String movie_features_rtn = "";
		
		for(int i = 0; i < u_features.split(",").length; i++){
			user_features[i] = Double.valueOf(u_features.split(",")[i]);
		}
		for(int i = 0; i < m_features.split(",").length; i++){
			movie_features[i] = Double.valueOf(m_features.split(",")[i]);
		}
		
		if(user_features.length != MAX_FEATURES ||
				movie_features.length != MAX_FEATURES)
			return null;
		
		double cf, mf;
		
		double p = predictRating(user_id, movie_id, user_features, movie_features);
		double err = rating-p;
		
		
		
		for(int index = 0;index < MAX_FEATURES; index++)
		{
			err = rating-p;

			cf = user_features[index];
			mf = movie_features[index];
			
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
			user_features_rtn += String.valueOf(user_feature) + ",";
			movie_features_rtn += String.valueOf(movie_feature) + ",";
		}
		
		
		return user_features_rtn.substring(0, user_features_rtn.length() - 2).trim() 
				+ "|" 
				+ user_features_rtn.substring(0, movie_features_rtn.length() - 2).trim();
	}
	
	private double predictRating(long user_id, int movie_id, double[] u_features, double[] m_features){
		double returnPrediction = 1;

		//Baseline Considerations:
		//average + baselineMovie[movieId] + baselineUser[userId];

		for(int index = 0;index < Features.MAX_FEATURES;index++)
		{
			returnPrediction += u_features[index]
								* u_features[index];
			
		}
		if(returnPrediction > 5) returnPrediction = 5;
		if(returnPrediction < 1) returnPrediction = 1;
		return returnPrediction;
	}

}
