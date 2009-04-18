package csg339.mapreduce.predlearner.util;

import java.util.HashMap;

/**
 * 
 * @author jarodwen & Jacob
 *
 */
public class Globals {
	public static int numMovies = 17771;
	public static int numRatings = 100480507;
	
	public static int numFeatures = 4;
	public static float numPhases = 2;
	
	public static float initialValue = 0.1f; // initial feature value
	
	public static double K = 0.015;   // Regularization parameter used to minimize over-fitting
	public static double LRATE = 0.001; // Learning rate parameter

	public static String trainingPath = "/user/jarod/input";
	public static String trainingFile = "small_input";

	
	//public static String trainingPath = "/user/hadoop/projectJJ/input_small_jarod_0409/";
	//public static String trainingFile = "trainingsetfile.txt";
	
	public static HashMap<Integer,FeatureVector> ufvs = new HashMap<Integer,FeatureVector>();
	public static HashMap<Integer,FeatureVector> mfvs = new HashMap<Integer,FeatureVector>();
	public  static Integer counter = 0;
}
