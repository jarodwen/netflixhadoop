package singlemachine;

import java.io.IOException;
import java.util.HashMap;
import util.FeatureVector;

public class OutputBucket{
	
	//public double timer = 0;
	public long line = 0;
	//public double timer2 = 0;
	public HashMap<Long,FeatureVector> ufvs = new HashMap<Long,FeatureVector>();
	public HashMap<Integer,FeatureVector> mfvs = new HashMap<Integer,FeatureVector>();
	
	public OutputBucket(){}
	
	public void init(int[] ratingOrder, int i, int j) {
		
		//for(int m = i;m<j;m++)
		//{
		//	ufvs.put(SingleMachineMaster.users[ratingOrder[m]], new FeatureVector(Globals.numFeatures,0));
		//	mfvs.put(SingleMachineMaster.movies[ratingOrder[m]], new FeatureVector(Globals.numFeatures,0));
		//}
	}
	public void collectU(long id, FeatureVector arg1) throws IOException {
		FeatureVector[] f = {ufvs.get(id),arg1};
		ufvs.put(id, FeatureVector.sum(f));
	}
	public void collectM(int id, FeatureVector arg1) throws IOException {
		FeatureVector[] f = {mfvs.get(id),arg1};		
		mfvs.put(id, FeatureVector.sum(f));
	}

}
