package csg339.mapreduce.predlearner.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
	public class Rating implements Writable
	{
		public Rating(long user, int movie, short rating)
		{
			this.user = user;
			this.movie = movie;
			this.rating = rating;
		}
		public Rating(DataInput arg0)
		{
			try {
				this.readFields(arg0);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		private long user;
		private int movie;
		private short rating;
		public Long getUser() {
			return user;
		}
		public Integer getMovie() {
			return movie;
		}
		public Short getRating() {
			return rating;
		}
		public void readFields(DataInput arg0) throws IOException {
			user = arg0.readLong();
			movie = arg0.readInt();
			rating = arg0.readShort();
		}
		public void write(DataOutput arg0) throws IOException {
			arg0.writeLong(user);
			arg0.writeInt(movie);
			arg0.writeShort(rating);
			
		}
		
	}
