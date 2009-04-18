package csg339.mapreduce.predictor.dbinterface;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

public class RatingDBWritable implements Writable, DBWritable {
	
	private long user_id;
	private int movie_id;
	private short rating;
	private String u_features;
	private String m_features;
	
	public RatingDBWritable(){
		user_id = 0;
		movie_id = 0;
		rating = 0;
		u_features = "";
		m_features = "";
	}
	
	public RatingDBWritable(long _user_id, int _movie_id, short _rating, String _u_features, String _m_features){
		user_id = _user_id;
		movie_id = _movie_id;
		rating = _rating;
		u_features = _u_features;
		m_features = _m_features;
	}

	public void readFields(ResultSet resultSet) throws SQLException {
		user_id = resultSet.getLong("user_id");
		movie_id = resultSet.getInt("movie_id");
		rating = resultSet.getShort("rating");
		u_features = resultSet.getString("u_features");
		m_features = resultSet.getString("m_features");
		System.out.println("Get records:" + u_features);
	}

	public void write(PreparedStatement statement) throws SQLException {
		
	}
	
	public long get_user_id(){
		return user_id;
	}
	public int get_movie_id(){
		return movie_id;
	}
	public short get_rating(){
		return rating;
	}
	public String get_u_features(){
		return u_features;
	}
	public String get_m_features(){
		return m_features;
	}

	public void readFields(DataInput in) throws IOException {
		this.user_id = in.readLong();
		this.movie_id = in.readInt();
		this.rating = in.readShort();
		this.u_features = Text.readString(in);
		this.m_features = Text.readString(in);
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(user_id);
		out.writeInt(movie_id);
		out.writeShort(rating);
		Text.writeString(out, this.u_features);
		Text.writeString(out, this.m_features);
		
	}
}
