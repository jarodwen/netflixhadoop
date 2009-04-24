package csg339.mapreduce.predlearner.learner;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

//import csg339.mapreduce.predlearner.util.FeatureID;
import csg339.mapreduce.predlearner.util.FeatureVector;
import csg339.mapreduce.predlearner.util.Globals;

/**
 * The Combiner class, for part of the reducing jobs in a parallel
 * way.
 * @author jarod
 *
 */
public class LearnCombine implements
		Reducer<Text, FeatureVector, Text, FeatureVector> {

	/**
	 * Combiner methods, which is the same as the reducer, however which can be
	 * parallelized on each mapper node. It also has different input format from
	 * reducer.
	 */
	public void reduce(Text key, Iterator<FeatureVector> values,
			OutputCollector<Text, FeatureVector> output, Reporter reporter)
			throws IOException {
		//if (Globals.IS_DEBUG)
		//	System.out.println("Reduce task at" + key.getId() + " " + key.type);
		if (!values.hasNext()) {
			// empty
			if (Globals.IS_DEBUG)
				System.out.println("reduce failed, empty input");
			//output.collect(key, null);
		} else {
			FeatureVector current = values.next();
			double[] avg = new double[current.getSize()];
			int weightTotal = current.getWeight();
			int count = 1;

			for (int i = 0; i < avg.length; i++)
				avg[i] = current.get(i);

			// Get average
			while (values.hasNext()) {
				current = values.next();
				weightTotal += current.getWeight();
				count += 1;
				for (int i = 0; i < avg.length; i++)
					avg[i] += current.get(i);
			}

			for (int i = 0; i < avg.length; i++)
				avg[i] = avg[i] / weightTotal;

			FeatureVector newValue = new FeatureVector(avg, weightTotal);

			// Now we have averages for a given key, output it!
			output.collect(key, newValue);
		}
	}

	public void close() throws IOException {
		// mos.close();
		// ...
	}

	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub

	}

}
