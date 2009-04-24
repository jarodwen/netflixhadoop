package util;


public class FeatureVector {
	int weight;
	int size = Globals.numFeatures;
	double[] vector = new double[Globals.numFeatures];

	public FeatureVector()
	{
		weight = 0;
	}
	public FeatureVector(FeatureVector f)
	{
		vector = f.vector.clone();
		size = f.size;
		weight = f.weight;
	}
	public FeatureVector(double[] avg, int weightTotal) {
		vector = avg.clone();
		size = avg.length;
		weight = weightTotal;
	}
	public FeatureVector(float initialValue) {
		for(int i = 0; i < this.size;i++)
			vector[i] = initialValue;
		weight = 1;
	}
	public int getWeight() {
		return weight;
	}
	public void setWeight(int inWeight) {
		weight = inWeight;
	}


	public double get(int i) {
		if(Double.isNaN(vector[i]) || Double.isInfinite(vector[i]))
		{
			System.out.println("AA");
			try {
				throw new Exception("AR");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.exit(0);
			}
		}
		return vector[i];
	}
	public void set(int i,double j) {
		vector[i] = j;
	}
	public int getSize() {
		return size;
	}
	
	public static FeatureVector sum(FeatureVector[] fvs)
	{
		FeatureVector outF = new FeatureVector();
		int w = 0;
		for(FeatureVector inF : fvs)
		{
			if(inF == null)
				continue;
			for(int i = 0 ; i < Globals.numFeatures;i++)
			{
				outF.vector[i] += inF.get(i)*inF.weight;
			}
			w += inF.weight;
		}

		for(int i = 0 ; i < Globals.numFeatures;i++)
			outF.vector[i] = outF.vector[i]/w;
		outF.weight = w;
		return outF;
	}
	public void SanityCheck()
	{
		for(int i = 0 ; i < Globals.numFeatures;i++)
		{
			this.get(i);
		}
	}
	

}
