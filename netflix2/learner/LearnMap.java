package learner;

import java.io.IOException;

import singlemachine.OutputBucket;
import singlemachine.SingleMachineMaster;
import util.FeatureVector;
import util.Globals;

public class LearnMap implements Runnable
{
	OutputBucket out;
	int[] ratingOrder;
	int lowerRange;
	int upperRange;
	
	/**
	 * Constructor.
	 * @param ratingOrder
	 * @param out2
	 * @param i lower bounds
	 * @param j upper bounds
	 */
	public LearnMap(int[] ratingOrder, OutputBucket out2, int i, int j) {
		out = out2;
		this.ratingOrder = ratingOrder;
		this.lowerRange = i;
		this.upperRange = j;
		// TODO Auto-generated constructor stub
	}
	public void run()
	{

		out.init(ratingOrder, lowerRange, upperRange);
		//int rat = SingleMachineMaster.numRatings;
		try {
			for(int i = lowerRange; i < upperRange;i++)
			{
				int j = ratingOrder[i];
				long user = SingleMachineMaster.users[j];
				int movie = SingleMachineMaster.movies[j];
				this.learn(user,movie,SingleMachineMaster.ratings[j],new FeatureVector(Globals.mfvs.get(movie)),new FeatureVector(Globals.ufvs.get(user)));

			}
		} catch (IOException e) {
			e.printStackTrace();
		}


	}
	
	/**
	 * The training method, using the input of a rating, and also the feature
	 * vectors for the specific user and movie.
	 * @param user
	 * @param movie
	 * @param rating
	 * @param mfv
	 * @param ufv
	 * @throws IOException
	 */
	public void learn(long user, int movie, short rating,FeatureVector mfv,FeatureVector ufv) throws IOException {
		//long start;
		//start = System.nanoTime();
		double cf;
		double mf;
		double p = LearnMap.PredictRating(movie,user,mfv,ufv);
		double err = rating-p;

		//Baseline related:
		//          //obu = baselineUser[userIdHash];
		//          //obi = baselineMovie[movieId];
		//          //baselineUser[userIdHash] = baselineUser[userIdHash] + LRATE * (err - K * baselineUser[userIdHash]);
		//          //baselineMovie[movieId] = baselineMovie[movieId] + LRATE * (err - K * baselineMovie[movieId]);
		//          //p =  p - (obu + obi) +(baselineUser[userIdHash]+baselineMovie[movieId]);

		for(int f=0;f<Globals.numFeatures;f++)
		{
			err = rating-p;

			// Cache off old feature values
			cf = ufv.get(f);
			mf = mfv.get(f);

			// Cross-train the features
//			if(Double.isNaN(cf + Globals.LRATE *( err * mf - Globals.K * cf)))
//				System.out.println("::" + cf + " " + Globals.LRATE *( err * mf - Globals.K * cf) );
//			if(Double.isNaN(mf + Globals.LRATE *( err * cf - Globals.K * mf)) || Double.isInfinite(mf + Globals.LRATE *( err * cf - Globals.K * mf)))
//				System.out.println("::" + mf +" " + Globals.LRATE *( err * cf - Globals.K * mf));
			ufv.set(f,cf + Globals.LRATE *( err * mf - Globals.K * cf));
			mfv.set(f,mf + Globals.LRATE *( err * cf - Globals.K * mf));
			p = p - (cf* mf) + ufv.get(f) * mfv.get(f);
			if(p > 5)
				p = 5;
			else if (p < 1)
				p = 1;
		}
		//out.timer += (System.nanoTime() - start)/1000000000.0;
		//start = System.nanoTime();
		out.collectU(user, ufv);
		out.collectM(movie, mfv);
		//out.timer2 += (System.nanoTime() - start)/1000000000.0;

		//}
		//System.out.println("endMap");

	}

	/**
	 * Predict the rating based on the given feature vectors.
	 * @param movieId
	 * @param userId
	 * @param movieFeatures
	 * @param userFeatures
	 * @return
	 */
	static double PredictRating(int movieId, long userId,FeatureVector movieFeatures,FeatureVector userFeatures)
	{
		double returnPrediction = 1;

		//Baseline Considerations:
		//average + baselineMovie[movieId] + baselineUser[userId];

		for(int f = 0;f<Globals.numFeatures;f++)
		{
			returnPrediction += movieFeatures.get(f)* userFeatures.get(f);

		}
		if(returnPrediction > 5) returnPrediction = 5;
		if(returnPrediction < 1) returnPrediction = 1;
		return returnPrediction;
	}


}
