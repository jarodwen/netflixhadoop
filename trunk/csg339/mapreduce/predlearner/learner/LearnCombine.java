package csg339.mapreduce.predlearner.learner;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import csg339.mapreduce.predlearner.util.FeatureID;
import csg339.mapreduce.predlearner.util.FeatureVector;



public class LearnCombine implements Reducer<FeatureID, FeatureVector,FeatureID, FeatureVector>
{
	 //private MultipleOutputs mos;

	 //public void configure(JobConf conf) {
	 //...
	 //	 mos = new MultipleOutputs(conf);
	 //}

	 public void reduce(FeatureID key, Iterator<FeatureVector> values,
			 OutputCollector<FeatureID, FeatureVector> output, Reporter reporter)
	 throws IOException {
		 System.out.println("Reduce task at" + key.getId() + " " + key.type);
		 if(!values.hasNext())
		 {
			 //empty
			 System.out.println("reduce failed, empty input");
				output.collect(key, null);
		 }
		 else
		 {
			FeatureVector current = values.next();
			double[] avg = new double[current.getSize()];
			int weightTotal = current.getWeight();
			int count = 1;
			
			for(int i = 0 ; i < avg.length;i++)
				avg[i] = current.get(i);
			
			while(values.hasNext())
			{
				current = values.next();
				weightTotal += current.getWeight();
				count += 1;
				for(int i = 0 ; i < avg.length;i++)
					avg[i] += current.get(i);
			}

			

			for(int i = 0 ; i < avg.length;i++)
				avg[i] = avg[i]/weightTotal;
			
			FeatureVector newValue = new FeatureVector(avg, weightTotal);
			
			//Now we have averages for a given key, output it!	
			output.collect(key, newValue);
		 }
	 }

	 public void close() throws IOException {
		// mos.close();
		 //...
	 }

	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

 }
