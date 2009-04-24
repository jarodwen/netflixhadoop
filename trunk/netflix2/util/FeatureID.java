package util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FeatureID implements WritableComparable {
	public enum FeatureType { movieFeature,userFeature};
	private long idu;
	private int idm;
	public void setId(long id)
	{
		if(type.equals(FeatureType.userFeature))
		{
			idu = id;
		}
	}
	public void setId(int id)
	{
		if(type.equals(FeatureType.movieFeature))
		{
			idm = id;
		}
	}
	public long getUID()
	{
		return idu;
	}
	public int getMID()
	{
		return idm;
	}
	
	public FeatureType type;
	
	public FeatureID(){
		
		this.type = FeatureType.movieFeature;
		idm = 0;
		idu = 0;
		
	}
	
	public FeatureID(long id,FeatureType t)
	{
		this.type = t;
		if(type.equals(FeatureType.userFeature))
		{
			this.idu = id;
			this.idm = 0;
		}
		else
		{
			
		}
		
	}
	public FeatureID(int id,FeatureType t)
	{
		this.type = t;
		if(type.equals(FeatureType.movieFeature))
		{
			this.idm = id;
			idu = 0;
		}
		else
		{
			
		}
		
	}
	public boolean equals(FeatureID target)
	{
		if(target.type.equals(this.type) && target.idm == this.idm && target.idu == this.idu)
			return true;
		else
			return false;
	}
	public int hashCode()
	{
	    int hash = 1;
	    hash = hash * 31 + type.hashCode();
	    hash = (int) (hash * 31  + idu);
	    hash = (int) (hash * 31 + idm);
	    return hash;

	}
	public void readFields(DataInput arg0) throws IOException {
		idu = arg0.readLong();
		idm = arg0.readInt();
		if(arg0.readBoolean())
			type = FeatureType.movieFeature;
		else
			type = FeatureType.userFeature;
	}
	public void write(DataOutput arg0) throws IOException {
		arg0.writeLong(idu);
		arg0.writeInt(idm);
		arg0.writeBoolean(type.equals(FeatureType.movieFeature));
		
	}
	public int compareTo(Object o) {
		if(o instanceof FeatureID)
		{
			FeatureID f = (FeatureID) o;
			if(this.equals(f))
				return 1;
			else
				return 0;
		}
		return 0;
	}
}
