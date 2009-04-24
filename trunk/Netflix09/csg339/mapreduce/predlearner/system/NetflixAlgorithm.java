package csg339.mapreduce.predlearner.system;

import csg339.mapreduce.predlearner.learner.LearnJob;
import csg339.mapreduce.predlearner.util.Globals;

public class NetflixAlgorithm {
	public static void main(String[] args) throws Exception {
		// For the case that we already have run some steps, here we can
		// start from the specific step.
		if(Globals.START_FROM_STEP < 0 || Globals.START_FROM_STEP > 2)
			Globals.START_FROM_STEP = 0;
		System.out.println("Begin Load and Randomize.");
		//First, read in a large body of text, and randomly give each line a node.
		if(Globals.START_FROM_STEP > 0)
			System.out.println("Get randomized data from previous jobs.");
		else
			csg339.mapreduce.loader.Main.main(null);
		System.out.println("Begin initializer for feature vectors.");
		if(Globals.START_FROM_STEP > 1)
			System.out.println("Get initial feature vectors from previous jobs.");
		else
			csg339.mapreduce.featureinit.Main.main(null);
		System.out.println("Beginning Learning.");
		//Second, for a number of phases:
		//Learn on that body of text.
		for(int i = 0; i<Globals.numPhases;i++)
		{
			Globals.counter+= 10;
			LearnJob.learn(i);
			System.out.println("Learn Epoch Completed." + Globals.counter);
			
		}
		System.out.println("Learning Completed.");
	}
}
