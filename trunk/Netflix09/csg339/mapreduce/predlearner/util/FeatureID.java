package csg339.mapreduce.predlearner.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FeatureID implements WritableComparable<Object> {
	
	/* The enum type of the id type */
	public enum FeatureType { movieFeature,userFeature};
	
	/* The actual id */
	private int id;
	
	/* The type of the id */
	public FeatureType type;
	
	public FeatureID(){
		
		this.type = FeatureType.movieFeature;
		id = 0;
		
	}
	
	public FeatureID(int id, FeatureType t)
	{
		this.type = t;
		this.id = id;
	}
	
	public void setId(int _id)
	{
		id = _id;
	}

	public int getId()
	{
		return id;
	}
	
	public void readFields(DataInput arg0) throws IOException {
		id = arg0.readInt();
		if(arg0.readInt() == 1)
			type = FeatureType.movieFeature;
		else
			type = FeatureType.userFeature;
	}
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(id);
		if(type.equals(FeatureType.movieFeature))
			arg0.writeInt(1);
		else
			arg0.writeInt(0);
		
	}
	public int compareTo(Object o) {
		if(o instanceof FeatureID)
		{
			FeatureID f = (FeatureID) o;
			if(this.equals(f))
				return 0;
			else
			{
				if(this.id < f.id)
					return -1;
				else
					return 1;
			}
		}
		return -1;
	}
	public boolean equals(FeatureID target)
	{
		if(target.type == this.type && target.id == this.id)
			return true;
		else
			return false;
	}
	public int hashCode()
	{
	    int hash = 1;
	    hash = hash * 31 + type.hashCode();
	    hash = (int) (hash * 31  + id);
	    return hash;

	}
	public String toString(){
		return String.valueOf(id) + "\t" + (this.type == FeatureType.userFeature?"0":"1");
	}
}
