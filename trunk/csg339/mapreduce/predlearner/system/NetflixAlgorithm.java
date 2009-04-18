package csg339.mapreduce.predlearner.system;

import csg339.mapreduce.predlearner.learner.LearnJob;
import csg339.mapreduce.predlearner.util.Globals;

public class NetflixAlgorithm {
	public static void main(String[] args) throws Exception {
		System.out.println("Beginning Learning.");
		//For a number of phases:
		for(int i = 0; i<Globals.numPhases;i++)
		{
			//First, read in a large body of text, and randomly give each line a node.
			//RandomizeJob.Randomize();
			//Second, learn on that body of text.
			Globals.counter+= 10;
			LearnJob.learn(i);
			System.out.println("Learn Epoch Completed." + Globals.counter);
			
		}
		System.out.println("Learning Completed.");
	}
}
