package system;

import singlemachine.SingleMachineMaster;
import util.Globals;

public class NetflixAlgorithm {
	public static void main(String[] args) throws Exception {
		System.out.println("Beginning Learning.");
		SingleMachineMaster.Load();
		long start = System.nanoTime();
		//For a number of phases:
		for(int i = 0; i<Globals.numPhases;i++)
		{
			System.out.println("Starting epoch.." + i);
			//First, read in a large body of text, and randomly give each line a node.
			//RandomizeJob.Randomize();
			//Second, learn on that body of text.
			//LearnJob.Learn();
			SingleMachineMaster.Learn();
			System.out.println("Learn Epoch Completed.");
		}
		System.out.println("Time:" + (System.nanoTime() - start)/1000000000.0);
		//System.out.println("Time Weight" + Globals.timer);
		//System.out.println("Time Weight2 " + Globals.timer2);
		//System.out.println("Time Weight3 " + Globals.timer3);
		
		System.out.println("Learning Completed.");
		Globals.writeFeatures();
		System.out.println("Writing Completed.");
	}
}
