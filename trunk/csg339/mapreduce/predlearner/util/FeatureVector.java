package csg339.mapreduce.predlearner.util;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class FeatureVector implements Writable {
	int weight;
	double[] vector;

	public FeatureVector()
	{
		vector = null;
		weight = 0;
	}
	public FeatureVector(double[] avg, int weightTotal) {
		vector = avg.clone();
		weight = weightTotal;
	}
	public FeatureVector(double[] avg) {
		vector = avg.clone();
		weight = 1;
	}
	public FeatureVector(int initialFeatures,float initialValue) {
		vector = new double[initialFeatures];
		for(int i = 0; i < initialFeatures;i++)
			vector[i] = initialValue;
		weight = 1;
	}
	public FeatureVector(DataInput arg0) {
		try {
			this.readFields(arg0);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}



	public int getWeight() {
		return weight;
	}
	public void setWeight(int inWeight) {
		weight = inWeight;
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
		weight = arg0.readInt();
		vector = new double[arg0.readInt()];
		for(int i = 0 ; i < vector.length;i++)
		{
			vector[i] = arg0.readDouble();
		}
		
	}
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(weight);
		arg0.writeInt(vector.length);
		for(int i = 0 ; i < vector.length;i++)
		{
			arg0.writeDouble(vector[i]);
		}
		
	}
	public double[] getVector() {
		return vector.clone();
	}
	public String toString()
	{
		String s = new String();
		s += weight + " " + vector.length + "\n";

		for(int i = 0 ; i < vector.length;i++)
		{
			s+= vector[i] + " ";
		}
		s += "\n";
		return s;
		
	}

}
