package csg339.mapreduce.loader;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.mapred.FileOutputFormat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * The customized output format, which only outputs the value of each 
 * data pair. Most of the code is just like the one in TextOutputFormat,
 * except for the ones when handling the key.
 * 
 * @author jarod
 *
 * @param <K>
 * @param <V>
 */
public class RatingOutputFormat<K, V> extends FileOutputFormat<K, V> {

	protected static class LineRecordWriter<K, V> implements RecordWriter<K, V> {
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
		private void writeObject(Object o) throws IOException {
			if (o instanceof Text) {
				Text to = (Text) o;
				out.write(to.getBytes(), 0, to.getLength());
			} else {
				out.write(o.toString().getBytes(utf8));
			}
		}

		public synchronized void write(K key, V value) throws IOException {

			boolean nullValue = value == null || value instanceof NullWritable;
			if (nullValue) {
				return;
			}
			if (!nullValue) {
				/* Only output the value here */
				writeObject(value);
			}
			out.write(newline);
		}

		public synchronized void close(Reporter reporter) throws IOException {
			out.close();
		}
	}

	public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job,
			String name, Progressable progress) throws IOException {
		boolean isCompressed = getCompressOutput(job);
		if (!isCompressed) {
			Path file = FileOutputFormat.getTaskOutputPath(job, name);
			FileSystem fs = file.getFileSystem(job);
			FSDataOutputStream fileOut = fs.create(file, progress);
			return new LineRecordWriter<K, V>(fileOut);
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
			return new LineRecordWriter<K, V>(new DataOutputStream(codec
					.createOutputStream(fileOut)));
		}
	}
}
