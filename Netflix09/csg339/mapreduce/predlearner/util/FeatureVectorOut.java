package csg339.mapreduce.predlearner.util;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class FeatureVectorOut implements Writable {
	double[] vector;

	public FeatureVectorOut()
	{
	}
	public FeatureVectorOut(double[] avg, int weightTotal) {
		vector = avg.clone();
	}
	public FeatureVectorOut(double[] avg) {
		vector = avg.clone();
	}
	public FeatureVectorOut(int initialFeatures,float initialValue) {
		vector = new double[initialFeatures];
		for(int i = 0; i < initialFeatures;i++)
			vector[i] = initialValue;
	}
	public FeatureVectorOut(DataInput arg0) {
		try {
			this.readFields(arg0);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	public double get(int i) {
		return vector[i];
	}
	public void set(int i,double j) {
		vector[i] = j;
	}

	public int getSize() {
		return vector.length;
	}
	public void readFields(DataInput arg0) throws IOException {
		vector = new double[arg0.readInt()];
		for(int i = 0 ; i < vector.length;i++)
		{
			vector[i] = arg0.readDouble();
		}
		
	}
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(vector.length);
		for(int i = 0 ; i < vector.length;i++)
		{
			arg0.writeDouble(vector[i]);
		}
		
	}
	public double[] getVector() {
		return vector.clone();
	}

}
