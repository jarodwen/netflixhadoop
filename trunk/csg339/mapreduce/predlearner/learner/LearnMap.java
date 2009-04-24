package csg339.mapreduce.predlearner.learner;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import csg339.mapreduce.predlearner.util.FeatureID;
import csg339.mapreduce.predlearner.util.FeatureVector;
import csg339.mapreduce.predlearner.util.Globals;

/**
 * LearnMap represents a map node and is composed of two parts: Configuration
 * Configuration loads the movie / user feature vector file into memory to be
 * used on learning
 * 
 * Mapping Mapping works over one user / movie / rating tuple, it utilizes their
 * feature vectors and outputs Trained feature vectors.
 * 
 * @author jarodwen & jakeouellette
 * 
 */
public class LearnMap implements
		Mapper<LongWritable, Text, Text, FeatureVector> {
	/** Local user feature vectors */
	private HashMap<Integer, double[]> ufvs = new HashMap<Integer, double[]>();
	//private double[] ufv = new double[Globals.numFeatures];

	/** Local movie feature vectors */
	private HashMap<Integer, double[]> mfvs = new HashMap<Integer, double[]>();
	//private double[] mfv = new double[Globals.numFeatures];;

	private Path inPath;;
	private JobConf inJobConf = new JobConf();
	
	/**
	 * Configure takes in a path in "learnmap.features.path" and parses it
	 */
	public void configure(JobConf arg0) {
		String pathStr = arg0.get("learnmap.features.path");
		inPath = new Path(pathStr);
		inJobConf = arg0;
		parseFeaturesFile();
		/*featureFiles = DistributedCache.getLocalCacheFiles(arg0);
		for(Path featureFile : featureFiles)
			parseFeaturesFile(featureFile);*/

	}
	
	/**
	 * Parse the feature vectors from the input file, which is defined
	 * in the Job Configuration.
	 * This method will only be called for one time when the mapper is 
	 * initialized.
	 */
	private void parseFeaturesFile(){
		try {

			BufferedReader fis = new BufferedReader(new InputStreamReader(
					FileSystem.get(inJobConf).open(inPath)));
			String line = null;
			while ((line = fis.readLine()) != null) {
				// By default the string tokens are separated by tab
				String tokenObjects[] = line.trim().split("\t");
				/*
				if(Integer.valueOf(tokenObjects[1]) == 1){
					if(Integer.valueOf(tokenObjects[0]) != user_id){
						continue;
					}
				}
				if(Integer.valueOf(tokenObjects[1]) == 0){
					if(Integer.valueOf(tokenObjects[0]) != movie_id){
						continue;
					}
				}*/
				if ((Globals.IS_DEBUG) && tokenObjects.length != (2 + Globals.numFeatures)) {
					// Do some exception test here
					System.err.println("Bad input file: ");
					System.out.println(line);
					System.out.println(tokenObjects.length + " is length");
					for (int i = 0; i < tokenObjects.length; i++)
						System.err.println(tokenObjects[i]);
					System.err.println("Bad input file:");
				}
				double[] feats = new double[Globals.numFeatures];

				for (int i = 0; i < tokenObjects.length && i < Globals.numFeatures; i++) {
					feats[i] = Double.valueOf(tokenObjects[i + 2]);
				}

				if (Integer.valueOf(tokenObjects[1]) == 1) { 
					// movie features
					//for (int i = 0; i < tokenObjects.length && i < Globals.numFeatures; i++) {
					//	mfv[i] = Double.valueOf(tokenObjects[i + 2]);
					//}
					mfvs.put(Integer.valueOf(tokenObjects[0]), feats);
				} else { 
					// user features
					//for (int i = 0; i < tokenObjects.length && i < Globals.numFeatures; i++) {
					//	ufv[i] = Double.valueOf(tokenObjects[i + 2]);
					//}
					ufvs.put(Integer.valueOf(tokenObjects[0]), feats);
				}

			}
			fis.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * The mapper trains off of one rating. It utilizes the movie and user
	 * feature vectors parsed by configuration
	 */
	public void map(LongWritable arg0, // Unused key
			Text ratingInput, // Input ratng
			OutputCollector<Text, // Target output
			FeatureVector> outCollector, // Target output object
			Reporter arg3) // Reporter (generally unused)
			throws IOException {
		if(Globals.IS_DEBUG) System.out.println("startMap");
		
		if(ratingInput.toString().trim() == "")
			return;

		// Parsing input text
		String tempString = ratingInput.toString();
		String tempElements[] = tempString.trim().split(",");

		// Parse in user, movie, rating tuple
		int user = Integer.valueOf(tempElements[0]);
		if(Globals.IS_LIMITED && user > Globals.USER_ID_THRESHOLD)
			return;
		int movie = Integer.valueOf(tempElements[1]);
		if(Globals.IS_LIMITED && movie > Globals.MOVIE_ID_THRESHOLD)
			return;
		short rating = Short.valueOf(tempElements[2]);

		FeatureID movieID = new FeatureID(movie,
				FeatureID.FeatureType.movieFeature);
		FeatureID userID = new FeatureID(user,
				FeatureID.FeatureType.userFeature);

		// Debug part
		/*
		if(Globals.IS_DEBUG) System.out.print(" UFVS:");
		for (Integer i : ufvs.keySet())
			if(Globals.IS_DEBUG) System.out.print(i + " ");
		if(Globals.IS_DEBUG) System.out.println("");

		if(Globals.IS_DEBUG) System.out.print(" MFVS:");
		for (Integer i : mfvs.keySet())
			if(Globals.IS_DEBUG) System.out.print(i + " ");
		if(Globals.IS_DEBUG) System.out.println("");

		if(Globals.IS_DEBUG) System.out.println("We want: u" + user + "m" + movie);

		if (ufvs.containsKey(user) != true)
			if(Globals.IS_DEBUG) System.out.println("issue");
		if (ufvs.get(user) == null)
			if(Globals.IS_DEBUG) System.out.println("issue2");
			*/
		// Debug part end
		
		double[] ufv = ufvs.get(user);
		double[] mfv = mfvs.get(movie);

		// double p = LearnMap.PredictRating(movie,
		// user,ratingSet.features.get(movieID),ratingSet.features.get(userID));
		double p = LearnMap.PredictRating(movie, user, mfv, ufv);
		double err; // = rating - p;

		// Baseline related: (unused)
		// //obu = baselineUser[userIdHash];
		// //obi = baselineMovie[movieId];
		// //baselineUser[userIdHash] = baselineUser[userIdHash] + LRATE * (err
		// - K * baselineUser[userIdHash]);
		// //baselineMovie[movieId] = baselineMovie[movieId] + LRATE * (err - K
		// * baselineMovie[movieId]);
		// //p = p - (obu + obi)
		// +(baselineUser[userIdHash]+baselineMovie[movieId]);

		double cachedUserFeature;
		double cachedMovieFeature;
		for (int f = 0; f < Globals.numFeatures; f++) {
			err = rating - p;

			// Cache off old feature values
			cachedUserFeature = ufv[f];
			cachedMovieFeature = mfv[f];

			// Cross-train the features
			// Train user
			// ratingSet.features.get(userID).set(f,Globals.LRATE *( err * mf -
			// Globals.K * cf));
			ufv[f] = ufv[f] + Globals.LRATE
					* (err * cachedMovieFeature - Globals.K * cachedUserFeature);
			if(Globals.IS_DEBUG) System.out.print(String.valueOf(ufv[f]));

			// Train movie
			// ratingSet.features.get(movieID).set(f,Globals.LRATE *( err * cf -
			// Globals.K * mf));
			mfv[f] = mfv[f] + Globals.LRATE
					* (err * cachedUserFeature - Globals.K * cachedMovieFeature);
			if(Globals.IS_DEBUG) System.out.print(String.valueOf(mfv[f]));

			// Cache off and truncate results
			p = p - (cachedUserFeature * cachedMovieFeature) + ufv[f] * mfv[f];
			if (p > 5)
				p = 5;
			else if (p < 1)
				p = 1;
		}
		// output to reducer/combiner
		if(Globals.IS_DEBUG) System.out.println("Mapper on key ("+userID.getId()+", 0)");
		outCollector.collect(new Text(userID.toString()), new FeatureVector(ufv));
		if(Globals.IS_DEBUG) System.out.println("Mapper on key ("+movieID.getId()+", 1)");
		outCollector.collect(new Text(movieID.toString()), new FeatureVector(mfv));
		if(Globals.IS_DEBUG) System.out.println("endMap");
	}

	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	static double PredictRating(int movieId, int userId,
			double[] movieFeatures, double[] userFeatures) {
		double returnPrediction = 1;

		// Baseline Considerations: (unused)
		// average + baselineMovie[movieId] + baselineUser[userId];

		// Predict based on features
		for (int f = 0; f < Globals.numFeatures; f++) {
			returnPrediction += movieFeatures[f] * userFeatures[f];

		}

		// Truncate results
		if (returnPrediction > 5)
			returnPrediction = 5;
		if (returnPrediction < 1)
			returnPrediction = 1;
		return returnPrediction;
	}
}
