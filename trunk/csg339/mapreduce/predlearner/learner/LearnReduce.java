package csg339.mapreduce.predlearner.learner;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import csg339.mapreduce.predlearner.util.FeatureID;
import csg339.mapreduce.predlearner.util.FeatureVector;



public class LearnReduce implements Reducer<FeatureID, FeatureVector,Text,Text>
{
	 //private MultipleOutputs mos;

	 //public void configure(JobConf conf) {
	 //...
	 //	 mos = new MultipleOutputs(conf);
	 //}

	 public void reduce(FeatureID key, Iterator<FeatureVector> values,
			 OutputCollector<Text,Text> output, Reporter reporter)
	 throws IOException {
		 System.out.println("Reduce task at" + key.getId() + " " + key.type);
		 //throw new IOException();
		 if(!values.hasNext())
		 {
			 //empty
			 System.out.println("reduce failed, empty input");
				output.collect(new Text(key.toString()), null);
		 }
		 else
		 {
			FeatureVector current = values.next();
			double[] avg = new double[current.getSize()];
			int weightTotal = current.getWeight();
			int count = 1;
			
			for(int i = 0 ; i < avg.length;i++)
				avg[i] = current.get(i);
			
			/**
			 * Construct the feature vector, using the average over all
			 * the features we have got from mapper.
			 */
			while(values.hasNext())
			{
				current = values.next();
				weightTotal += current.getWeight();
				count += 1;
				for(int i = 0 ; i < avg.length;i++)
					avg[i] += current.get(i);
			}
			// Get the average
			for(int i = 0 ; i < avg.length;i++)
				avg[i] = avg[i]/weightTotal;
			
			//Now we have averages for a given key, output it!
			
			String s = new String();

			for (int i = 0; i < avg.length; i++) {
				if(i != 0)
					s += "\t";
				s += avg[i];
			}
				output
						.collect(new Text(key.toString()), new Text(s));
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
