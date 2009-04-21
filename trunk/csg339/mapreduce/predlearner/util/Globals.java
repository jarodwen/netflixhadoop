package csg339.mapreduce.predlearner.util;

import java.util.HashMap;

/**
 * 
 * @author jarodwen & Jacob
 *
 */
public class Globals {
	// the number of movies, whose id is from 1 to 17770
	public static int numMovies = 17771;
	
	// the number of ratings
	public static int numRatings = 100480507;
	
	// the number of features we want to predict
	public static int numFeatures = 4;
	// the number of phrases our learner will run
	public static float numPhases = 2;
	
	// the initial value of the feature vector
	public static float initialValue = 0.1f; 
	
	public static double K = 0.015;   // Regularization parameter used to minimize over-fitting
	public static double LRATE = 0.001; // Learning rate parameter

	// the path for the input rating file
	public static String trainingPath = "/user/hadoop/projectJJ/input";
	public static String randomizedPath = "/user/hadoop/projectJJ/input_rand";
	public static String featPathPrefix = "/user/hadoop/projectJJ/input_feat_";
	public static String trainingFile = "small_input";
	
	// the debug flag
	public static boolean IS_DEBUG = true;
	
	// the threshold for testing on user id and movie id
	public static int USER_ID_THRESHOLD = 1000000000;
	public static int MOVIE_ID_THRESHOLD = 100000000;
	
	// the global hashmap used for original algorithm. decepted.
	public static HashMap<Integer,FeatureVector> ufvs = new HashMap<Integer,FeatureVector>();
	public static HashMap<Integer,FeatureVector> mfvs = new HashMap<Integer,FeatureVector>();
	public  static Integer counter = 0;
}
