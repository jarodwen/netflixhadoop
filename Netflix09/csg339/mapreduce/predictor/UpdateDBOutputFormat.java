package csg339.mapreduce.predictor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapred.lib.db.DBWritable;

public class UpdateDBOutputFormat<K  extends DBWritable, V> extends DBOutputFormat<K, V> {
	
	private static final Log LOG = LogFactory.getLog(UpdateDBOutputFormat.class);

	  /**
	   * Constructs the query used as the prepared statement to insert data.
	   * 
	   * @param table
	   *          the table to insert into
	   * @param fieldNames
	   *          the fields to insert into. If field names are unknown, supply an
	   *          array of nulls.
	   */
	
	  protected String constructQuery(String table, String[] fieldNames) {
	    if(fieldNames == null) {
	      throw new IllegalArgumentException("Field names may not be null");
	    }

	    StringBuilder query = new StringBuilder();
	    query.append("UPDATE ").append(table);
	    query.append(" SET");

	    if (fieldNames.length > 0 && fieldNames[0] != null) {
	      query.append(" (");
	      for (int i = 0; i < fieldNames.length; i++) {
	        query.append(fieldNames[i]).append("=").append("?");
	        if (i != fieldNames.length - 1) {
	          query.append(",");
	        }
	      }
	    }
	    query.append(";");
	    
	    LOG.debug(query.toString() + "\n");

	    return query.toString();
	  }
	
}
