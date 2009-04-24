package csg339.mapreduce.predlearner.learner;

import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import csg339.mapreduce.predlearner.util.*;


/**
 * Treats keys as offset in file and value as line. 
 */
public class NetflixRecordReader implements RecordReader<LongWritable, RatingUnit> {
  private CompressionCodecFactory compressionCodecs = null;
  private long start;
  private long pos;
  private long end;
  private LineReader in;
  int maxLineLength;

  /**
   * A class that provides a line reader from an input stream.
   * @deprecated Use {@link org.apache.hadoop.util.LineReader} instead.
   */
  @Deprecated
  public static class LineReader extends org.apache.hadoop.util.LineReader {
    LineReader(InputStream in) {
      super(in);
    }
    LineReader(InputStream in, int bufferSize) {
      super(in, bufferSize);
    }
    public LineReader(InputStream in, Configuration conf) throws IOException {
      super(in, conf);
    }
  }

  public NetflixRecordReader(Configuration job, 
                          FileSplit split) throws IOException {
    this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
                                    Integer.MAX_VALUE);
    start = split.getStart();
    end = start + split.getLength();
    final Path file = split.getPath();
    compressionCodecs = new CompressionCodecFactory(job);
    final CompressionCodec codec = compressionCodecs.getCodec(file);

    // open the file and seek to the start of the split
    FileSystem fs = file.getFileSystem(job);
    FSDataInputStream fileIn = fs.open(split.getPath());
    boolean skipFirstLine = false;
    if (codec != null) {
      in = new LineReader(codec.createInputStream(fileIn), job);
      end = Long.MAX_VALUE;
    } else {
      if (start != 0) {
        skipFirstLine = true;
        --start;
        fileIn.seek(start);
      }
      in = new LineReader(fileIn, job);
    }
    if (skipFirstLine) {  // skip first line and re-establish "start".
      start += in.readLine(new Text(), 0,
                           (int)Math.min((long)Integer.MAX_VALUE, end - start));
    }
    this.pos = start;
  }
  
  public NetflixRecordReader(InputStream in, long offset, long endOffset,
                          int maxLineLength) {
    this.maxLineLength = maxLineLength;
    this.in = new LineReader(in);
    this.start = offset;
    this.pos = offset;
    this.end = endOffset;    
  }

  public NetflixRecordReader(InputStream in, long offset, long endOffset, 
                          Configuration job) 
    throws IOException{
    this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
                                    Integer.MAX_VALUE);
    this.in = new LineReader(in, job);
    this.start = offset;
    this.pos = offset;
    this.end = endOffset;    
  }
  
  public LongWritable createKey() {
    return new LongWritable();
  }
  
  public RatingUnit createValue() {
    return new RatingUnit();
  }
  
  /** Read a line. */
  public synchronized boolean next(LongWritable key, RatingUnit value)
    throws IOException {
	  Globals.counter ++;
	  if(Globals.IS_DEBUG) System.out.println("StartNext");
    while (pos < end) {
      key.set(pos);
      Text temp = new Text();
      int newSize = in.readLine(temp, maxLineLength,
                               Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),
                                         maxLineLength));
      //Parse line from file
      //Find delimiters
      String tempString = temp.toString();
      String tempElements[] = tempString.trim().split(",");
      //int firstDelimiter = tempString.indexOf(",", 0);
      //int secondDelimiter = tempString.indexOf(",", firstDelimiter);
      //int thirdDelimiter = tempString.indexOf("\n", secondDelimiter);
      //Parse in user, movie, rating tuple
      if(tempElements.length != 3){
    	  
      }
      int u = Integer.parseInt(tempElements[0]);
      int m = Integer.parseInt(tempElements[1]);
      short r = Short.parseShort(tempElements[2]);
      //Create the rating object
      value.r = new Rating(u,m,r);
      
      if(Globals.ufvs.containsKey(u));
      	Globals.ufvs.put(u, new FeatureVector(Globals.numFeatures,Globals.initialValue));
      if(Globals.mfvs.containsKey(m));
	  	Globals.mfvs.put(m, new FeatureVector(Globals.numFeatures,Globals.initialValue));
      	
      value.ufv = Globals.ufvs.get(u);
      value.mfv = Globals.mfvs.get(m);
      
      
      if (newSize == 0) {
    	  if(Globals.IS_DEBUG) System.out.println("endNext");
        return false;
      }
      pos += newSize;
      if (newSize < maxLineLength) {
    	  if(Globals.IS_DEBUG) System.out.println("endNext2");
        return true;
      }

    }
    if(Globals.IS_DEBUG) System.out.println("endNext3");
    return false;
  }

  /**
   * Get the progress within the split
   */
  public float getProgress() {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float)(end - start));
    }
  }
  
  public  synchronized long getPos() throws IOException {
    return pos;
  }

  public synchronized void close() throws IOException {
    if (in != null) {
      in.close(); 
    }
  }
}