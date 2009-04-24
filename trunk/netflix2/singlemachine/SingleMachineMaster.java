package singlemachine;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Locale;
import java.util.Random;
import java.util.regex.Pattern;
import java.util.Scanner;

import learner.LearnMap;

import util.*;


/** Handles loading of data, and tasking of distribution of learning across threads.
 * 
 * This class is working as a job tracker in a hadoop system.
 * 
 * @author jake
 *
 */
public class SingleMachineMaster {
	
	public static long[] users = new long[Globals.numRatings];
	public static int[] movies = new int[Globals.numRatings];
	public static short[] ratings = new short[Globals.numRatings];
	public static int numRatings = 0;
	
	/**
	 * Loads a file from globally defined paths
	 */
	public static void Load()
	{
		//Load data
		File d = new File(Globals.trainingPath + Globals.trainingFile);
		if(d.isFile())
		{
			System.out.println("Loading File from flat file: " +d.getName()+" ...");
            Scanner s;
			try {
				s = new Scanner(
				        new BufferedReader(new FileReader(d)));
	            s.useLocale(Locale.US);
	            
	            Pattern p = Pattern.compile(",|\\s+");
	            s.useDelimiter(p);
	            while(s.hasNextLong())
	            {
	            	long user = s.nextLong();
	            	int movie = s.nextInt();
	            	Short rating = s.nextShort();
            		users[numRatings] = user;
            		movies[numRatings] = movie;
            		ratings[numRatings] = rating;
	            	numRatings++;
	            	if(numRatings % (Globals.numRatings / 100) == 0)
            		{
            			System.out.println(numRatings / (Globals.numRatings / 100) + "%");
            		}
	            	else if(numRatings % (Globals.numRatings / 1000)   == 0)
	            	{
        				System.out.print(".");
	            	}
//	            	if(numRatings > 2268082)
//	            		break;
	            }
	            
	            for(Long user : users)
	            {
	            	if(user == 0)
	            		break;
	            	if(!Globals.ufvs.containsKey(user))
	            	{
	            		Globals.ufvs.put(user, new FeatureVector(Globals.initialValue));
	            	}
            	}
	            for(Integer movie : movies)
	            {
	            	if(movie == 0)
	            		break;
	            	if(!Globals.mfvs.containsKey(movie))
	            		Globals.mfvs.put(movie, new FeatureVector(Globals.initialValue));
	            }
			}
			catch (FileNotFoundException e) {
				e.printStackTrace();
			}

		}
	}
	
	/**
	 * Splits learning process into several threads (internally),
	 * learns and updates Global feature vector.
	 * @throws IOException
	 */
	public static void Learn() throws IOException
	{
		System.out.println("Learning ..");

		//Order initialization
		int[] ratingOrder = new int[numRatings];
		for(int i = 0;i < ratingOrder.length;i++)
			ratingOrder[i] = i;
		shuffle(ratingOrder);

		long start = System.nanoTime();
		//Thread Initialization
		int numThreads = Globals.numThreads;
		int partitionSize = numRatings/numThreads;
		Thread[] threadList = new Thread[numThreads];
		OutputBucket[] buckets = new OutputBucket[numThreads];
		
		for(int j = 0; j < numThreads-1;j++)
		{
			buckets[j] =  new OutputBucket();
			threadList[j] = (new Thread(new LearnMap(ratingOrder,buckets[j],partitionSize*j,partitionSize*(j+1))));
		}
		buckets[numThreads-1] = new OutputBucket();
		threadList[numThreads-1] = (new Thread(new LearnMap(ratingOrder,buckets[numThreads-1],partitionSize*(numThreads-1),numRatings)));

		//Start threads
		for(Thread t :threadList)
			t.start();
		//Wait for completion
		for(Thread t :threadList)
		{
			try {
				t.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}	

		for(long i : Globals.ufvs.keySet())
		{
			FeatureVector[] fvs = new FeatureVector[buckets.length];
			int ptr = 0;
			for(OutputBucket b : buckets)
				if(b.ufvs.containsKey(i))
				{
					fvs[ptr] = b.ufvs.get(i);
					ptr++;
				}
			FeatureVector f =  FeatureVector.sum(fvs);
			f.setWeight(1);
			Globals.ufvs.put(i,f );
		}
		for(int i : Globals.mfvs.keySet())
		{
			FeatureVector[] fvs = new FeatureVector[buckets.length];
			int ptr = 0;
			for(OutputBucket b : buckets)
				if(b.mfvs.containsKey(i))
				{
					fvs[ptr] = b.mfvs.get(i);
					ptr++;
				}
			FeatureVector f =  FeatureVector.sum(fvs);
			f.setWeight(1);
			Globals.mfvs.put(i,f );
		}
		

		Globals.timer += (System.nanoTime() - start)/1000000000.0;
			
	}
	
	/**
	 * Randomize the order of elements in the given array.
	 * @param array
	 */
	public static void shuffle (int[] array) 
    {
        Random rng = new Random();   // i.e., java.util.Random.
        int n = array.length;        // The number of items left to shuffle (loop invariant).
        while (n > 1) 
        {
            int k = rng.nextInt(n);  // 0 <= k < n.
            n--;                     // n is now the last pertinent index;
            int temp = array[n];     // swap array[n] with array[k] (does nothing if k == n).
            array[n] = array[k];
            array[k] = temp;
        }
    }
}
