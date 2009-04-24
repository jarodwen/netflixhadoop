package csg339.mapreduce.predlearner.learner;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

import csg339.mapreduce.predlearner.util.FeatureID;
import csg339.mapreduce.predlearner.util.FeatureVector;
import csg339.mapreduce.predlearner.util.Globals;


public class NetflixOutputFormat  extends FileOutputFormat<FeatureID, FeatureVector> {
	protected static class LineRecordWriter  implements RecordWriter<FeatureID, FeatureVector> {
		private static final String utf8 = "UTF-8";

		private static final byte[] newline;
		static {
			try {
				newline = "\n".getBytes(utf8);
			} catch (UnsupportedEncodingException uee) {
				throw new IllegalArgumentException("can't find " + utf8
						+ " encoding");
			}
		}

		protected DataOutputStream out;


		public LineRecordWriter(DataOutputStream out) {
			this.out = out;
		}

		/**
		 * Write the object to the byte stream, handling Text as a special
		 * case.
		 * @param o the object to print
		 * @throws IOException if the write throws, we pass it on
		 */
		@SuppressWarnings("unused")
		private void writeObject(Object o) throws IOException {
			if (o instanceof Text) {
				Text to = (Text) o;
				out.write(to.getBytes(), 0, to.getLength());
			} else {
				out.write(o.toString().getBytes(utf8));
			}
		}

		public synchronized void write(FeatureID key, FeatureVector value) throws IOException {
			if(Globals.IS_DEBUG) System.out.println("outStart");
			Globals.ufvs.put(key.getId(), value);
			if(Globals.IS_DEBUG) System.out.println(key.getId() + " "+value.toString());
			Globals.counter++;
			if(Globals.IS_DEBUG) System.out.println(Globals.counter);
			/*
			boolean nullValue = value == null || value instanceof NullWritable;
			if (nullValue) {
				return;
			}
			if (!nullValue) {
				writeObject(value);
			}
			out.write(newline);*/
		}

		public synchronized void close(Reporter reporter) throws IOException {
			if(out != null)
				out.close();
		}
	}

	public RecordWriter<FeatureID, FeatureVector> getRecordWriter(FileSystem ignored, JobConf job,
			String name, Progressable progress) throws IOException {
		if(Globals.IS_DEBUG) System.out.println("startGiveRecordReader");
		return new LineRecordWriter(null);
		/*
		boolean isCompressed = getCompressOutput(job);
		if (!isCompressed) {
			Path file = FileOutputFormat.getTaskOutputPath(job, name);
			FileSystem fs = file.getFileSystem(job);
			
			FSDataOutputStream fileOut = fs.create(file, progress);
			return new LineRecordWriter(fileOut);
		} else {
			Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(
					job, GzipCodec.class);
			// create the named codec
			CompressionCodec codec = ReflectionUtils.newInstance(codecClass,
					job);
			// build the filename including the extension
			Path file = FileOutputFormat.getTaskOutputPath(job, name
					+ codec.getDefaultExtension());
			FileSystem fs = file.getFileSystem(job);
			FSDataOutputStream fileOut = fs.create(file, progress);
			return new LineRecordWriter(new DataOutputStream(codec
					.createOutputStream(fileOut)));
		}*/
	}
}


