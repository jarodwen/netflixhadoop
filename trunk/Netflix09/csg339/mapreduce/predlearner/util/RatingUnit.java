package csg339.mapreduce.predlearner.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class RatingUnit implements Writable {
	public Rating r;
	public FeatureVector mfv;
	public FeatureVector ufv;
	public void readFields(DataInput arg0) throws IOException {
		r = new Rating(arg0);
		mfv = new FeatureVector(arg0);
		ufv = new FeatureVector(arg0);
	}
	public void write(DataOutput arg0) throws IOException {
		r.write(arg0);
		mfv.write(arg0);
		ufv.write(arg0);
		
		
	}
	
	
}
