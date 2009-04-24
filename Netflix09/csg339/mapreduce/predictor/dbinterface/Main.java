package csg339.mapreduce.predictor.dbinterface;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;

public class Main {

	public static final int MAX_ITERATIONS = 2;

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		/* MapReduce configuration part */
		JobConf conf = new JobConf(Main.class);

		conf.setJobName("RatingPred");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(FeaturesDBWritable.class);

		conf.setMapperClass(PredMapper.class);
		conf.setReducerClass(PredReducer.class);

		conf.setInputFormat(DBInputFormat.class);

		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver",
				"jdbc:mysql://localhost:3306/netflix", "hadoop", "hadoop");
		DBInputFormat
				.setInput(
						conf,
						RatingDBWritable.class,
						"SELECT T1.user_id user_id, T1.movie_id movie_id, T1.rating rating, T2.features u_features, T3.features m_features "
								+ "FROM Ratings AS T1, Features AS T2, Features AS T3 "
								+ "WHERE T2.um_type = TRUE AND T2.id = T1.user_id "
								+ "AND T3.um_type = FALSE AND T3.id = T1.movie_id ",
						"SELECT COUNT(*) FROM ("
								+ "SELECT T1.user_id user_id, T1.movie_id movie_id, T1.rating rating, T2.features u_features, T3.features m_features "
								+ "FROM Ratings AS T1, Features AS T2, Features AS T3 "
								+ "WHERE T2.um_type = TRUE AND T2.id = T1.user_id "
								+ "AND T3.um_type = FALSE AND T3.id = T1.movie_id "
								+ ") AS T");

		conf.setOutputFormat(UpdateDBOutputFormat.class);
		UpdateDBOutputFormat.setOutput(conf, "Features", "id, um_type, features");
		JobClient.runJob(conf);

	}

}