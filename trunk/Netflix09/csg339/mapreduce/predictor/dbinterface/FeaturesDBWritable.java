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

public class FeaturesDBWritable implements Writable, DBWritable {
	
	static int MAX_FEATURES = 3;
	
	private int id;
	private boolean um_type;
	private String features;
	
	public FeaturesDBWritable(int _id, boolean _um_type, String _features){
		id = _id;
		um_type = _um_type;
		features = _features;
	}

	public void write(PreparedStatement statement) throws SQLException {
		statement.setInt(1, id);
		statement.setBoolean(2, um_type);
		statement.setString(3, features);

	}
	
	public void set_id(int _id){
		if(_id < 0)
			return;
		else
			id = _id;
	}
	
	public void set_um_type(boolean _um_type){
		um_type = _um_type;
	}
	
	public void set_features(String _features){
		if(_features.split(",").length != MAX_FEATURES)
			return;
		else
			features = _features;
	}
	
	public String get_features(){
		return features;
	}

	public void readFields(ResultSet arg0) throws SQLException {
		id = arg0.getInt("id");
		um_type = arg0.getBoolean("um_type");
		features = arg0.getString("features");
		
	}

	public void readFields(DataInput in) throws IOException {
		id = in.readInt();
		um_type = in.readBoolean();
		features = Text.readString(in);
		
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeBoolean(um_type);
		Text.writeString(out, features);
		
	}

}
