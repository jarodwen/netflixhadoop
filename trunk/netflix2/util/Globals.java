package util;

import java.text.DecimalFormat;
import java.util.HashMap;

import java.io.*;


public class Globals {
	public static double timer = 0;
	public static double timer2 = 0;
	public static double timer3 = 0;
	public static final int numMovies = 17771;
	public static final int numRatings = 3000000;
	
	/**
	 * The number of features we want to train.
	 */
	public static final int numFeatures = 4;
	
	/**
	 * The total number of phases we will run.
	 */
	public static final float numPhases = 2;
	
	/**
	 * The number of threads we have.
	 */
	public static int numThreads = 3;
	
	public static final float initialValue = 0.1f; // initial feature value
	
	public static final double K = 0.015;   // Regularization parameter used to minimize over-fitting
	public static final double LRATE = 0.001; // Learning rate parameter

	//public static String trainingPath = "///user/hadoop/projectJJ/small_input/";
	//public static String trainingFile = "tsmall.txt";
	
	/**
	 * The path to the training data
	 */
	public static final String trainingPath = "/home/jarod/test_ouput/";
	
	/**
	 * The training data file
	 */
	public static final String trainingFile = "tdata_1000";
	
	public static final HashMap<Long,FeatureVector> ufvs = new HashMap<Long,FeatureVector>();
	public static final HashMap<Integer,FeatureVector> mfvs = new HashMap<Integer,FeatureVector>();
	
	/**
	 * Write the features trained to local files in the same training path.
	 */
	public static void writeFeatures()
	{
		/**
		 * DecimalFormat is used to specify the format of the feature value.
		 */
		DecimalFormat formatter = new DecimalFormat("#.######");
		int n = 0;
		try {
			
			File f = new File(Globals.trainingPath + "java_out_user" + n);
			File f2 = new File(Globals.trainingPath + "java_out_movie" + n);
			while(f.exists() || f2.exists())
			{
				n++;
				f = new File(Globals.trainingPath + "java_out_user" + n);
				f2 = new File(Globals.trainingPath + "java_out_movie" + n);
			}
			
	        BufferedWriter out = new BufferedWriter(new FileWriter(f));
			for(long u : Globals.ufvs.keySet())
			{
				out.write("user " + u + " \n");
				for(int i = 0; i < Globals.numFeatures;i++)
				{
					out.write(formatter.format(Globals.ufvs.get(u).get(i))  + " ");
				}
				out.write("\n");
			}
			out.close();
			
	        BufferedWriter out2 = new BufferedWriter(new FileWriter(f2));
			for(int m : Globals.mfvs.keySet())
			{
				out2.write("movie " + m + " \n");
				for(int i = 0; i < Globals.numFeatures;i++)
					out2.write(formatter.format(Globals.mfvs.get(m).get(i))  + " ");
				out2.write("\n");
			}
			out2.close();
	    } catch (IOException e) {
	    }
	}
}
