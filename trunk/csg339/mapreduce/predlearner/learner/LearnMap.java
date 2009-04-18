package csg339.mapreduce.predlearner.learner;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.fs.FSDataInputStream;
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
		Mapper<LongWritable, Text, FeatureID, FeatureVector> {
	/** Local user feature vectors */
	private HashMap<Integer, double[]> ufvs = new HashMap<Integer, double[]>();

	/** Local movie feature vectors */
	private HashMap<Integer, double[]> mfvs = new HashMap<Integer, double[]>();

	/**
	 * Configure takes in a path in "learnmap.features.path" and parses it
	 */
	public void configure(JobConf arg0) {
		String pathStr = arg0.get("learnmap.features.path");
		try {
			FSDataInputStream inStream = FileSystem.get(arg0).open(
					new Path(pathStr));
			parseFeaturesFile(inStream);
			inStream.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * Parsing a feature file, the file is in the format: ID Type Feat1 Feat2
	 * Featn-1 Featn
	 * 
	 * Where type is a boolean defined by: 0 for movie, 1 for user.
	 */
	private void parseFeaturesFile(FSDataInputStream inStream) {
		try {

			BufferedReader fis = new BufferedReader(new InputStreamReader(
					inStream));
			String line = null;
			while ((line = fis.readLine()) != null) {

				String tokenObjects[] = line.trim().split("\t");
				if (tokenObjects.length != (2 + Globals.numFeatures)) {
					// Do some exception test here
					System.err.println("Bad input file: ");
					System.out.println(line);
					System.out.println(tokenObjects.length + " is length");
					for (int i = 0; i < tokenObjects.length; i++)
						System.err.println(tokenObjects[i]);
					System.err.println("Bad input file:");
				}
				double[] feats = new double[Globals.numFeatures];

				for (int i = 2; i < tokenObjects.length; i++) {
					feats[i] = Double.valueOf(tokenObjects[i]);
				}

				if (Integer.valueOf(tokenObjects[1]) == 1) { // movie
																	// features
					mfvs.put(Integer.valueOf(tokenObjects[0]), feats);
				} else { // user features
					ufvs.put(Integer.valueOf(tokenObjects[0]), feats);
				}

			}
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
			OutputCollector<FeatureID, // Target output
			FeatureVector> outCollector, // Target output object
			Reporter arg3) // Reporter (generally unused)
			throws IOException {
		System.out.println("startMap");

		// Parsing input text
		String tempString = ratingInput.toString();
		String tempElements[] = tempString.trim().split(",");

		// Parse in user, movie, rating tuple
		int user = Integer.valueOf(tempElements[0]);
		int movie = Integer.valueOf(tempElements[1]);
		short rating = Short.valueOf(tempElements[2]);

		FeatureID movieID = new FeatureID(movie,
				FeatureID.FeatureType.movieFeature);
		FeatureID userID = new FeatureID(user,
				FeatureID.FeatureType.userFeature);

		System.out.print(" UFVS:");
		for (Integer i : ufvs.keySet())
			System.out.print(i + " ");
		System.out.println("");

		System.out.print(" MFVS:");
		for (Integer i : mfvs.keySet())
			System.out.print(i + " ");
		System.out.println("");

		System.out.println("We want: u" + user + "m" + movie);

		if (ufvs.containsKey(user) != true)
			System.out.println("issue");
		if (ufvs.get(user) == null)
			System.out.println("issue2");
		double[] ufv = ufvs.get(user);
		double[] mfv = mfvs.get(movie);

		// double p = LearnMap.PredictRating(movie,
		// user,ratingSet.features.get(movieID),ratingSet.features.get(userID));
		double p = LearnMap.PredictRating(movie, user, mfv, ufv);
		double err = rating - p;

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
			ufv[f] = Globals.LRATE
					* (err * cachedMovieFeature - Globals.K * cachedUserFeature);

			// Train movie
			// ratingSet.features.get(movieID).set(f,Globals.LRATE *( err * cf -
			// Globals.K * mf));
			mfv[f] = Globals.LRATE
					* (err * cachedUserFeature - Globals.K * cachedMovieFeature);

			// Cache off and truncate results
			p = p - (cachedUserFeature * cachedMovieFeature) + ufv[f] * mfv[f];
			if (p > 5)
				p = 5;
			else if (p < 1)
				p = 1;
		}
		// output to a file
		System.out.println("Mapper on key ("+userID.getId()+", 0)");
		outCollector.collect(userID, new FeatureVector(ufv));
		System.out.println("Mapper on key ("+movieID.getId()+", 1)");
		outCollector.collect(movieID, new FeatureVector(mfv));
		System.out.println("endMap");
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
