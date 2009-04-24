package singlemachine;

import java.io.IOException;
import java.util.HashMap;
import util.FeatureVector;

/**
 * An output bucket will contain the output of a give mapper, which will be later
 * used by the reducer for combination. 
 * @author jake
 *
 */
public class OutputBucket{
	
	//public double timer = 0;
	public long line = 0;
	//public double timer2 = 0;
	/**
	 * The container for user feature vectors.
	 */
	public HashMap<Long,FeatureVector> ufvs = new HashMap<Long,FeatureVector>();
	/**
	 * The container for movie feature vectors.
	 */
	public HashMap<Integer,FeatureVector> mfvs = new HashMap<Integer,FeatureVector>();
	
	/**
	 * Constructor.
	 */
	public OutputBucket(){}
	
	/**
	 * Initializer.
	 * @param ratingOrder
	 * @param i
	 * @param j
	 */
	public void init(int[] ratingOrder, int i, int j) {
		
		//for(int m = i;m<j;m++)
		//{
		//	ufvs.put(SingleMachineMaster.users[ratingOrder[m]], new FeatureVector(Globals.numFeatures,0));
		//	mfvs.put(SingleMachineMaster.movies[ratingOrder[m]], new FeatureVector(Globals.numFeatures,0));
		//}
	}
	/**
	 * Collect the given user feature vector
	 * @param id
	 * @param arg1
	 * @throws IOException
	 */
	public void collectU(long id, FeatureVector arg1) throws IOException {
		FeatureVector[] f = {ufvs.get(id),arg1};
		ufvs.put(id, FeatureVector.sum(f));
	}
	/**
	 * Collect the given movie feature vector
	 * @param id
	 * @param arg1
	 * @throws IOException
	 */
	public void collectM(int id, FeatureVector arg1) throws IOException {
		FeatureVector[] f = {mfvs.get(id),arg1};		
		mfvs.put(id, FeatureVector.sum(f));
	}

}
