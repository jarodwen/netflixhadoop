package csg339.mapreduce.predictor.newtest;

import java.io.Serializable;
import java.util.Vector;

class Features implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -192133346304176048L;

	static final int MAX_FEATURES = 3;
	
	static final String ratings = "1488844,1,3" +
			"\n822109,1,5" +
			"\n885013,1,4" +
			"\n30878,1,4" +
			"\n823519,1,3" +
			"\n893988,1,3" +
			"\n124105,1,4" +
			"\n1248029,1,3" +
			"\n1842128,1,4" +
			"\n2238063,1,3";
	
	private Vector<UserFeatures> users = new Vector<UserFeatures>();
	private Vector<MovieFeatures> movies = new Vector<MovieFeatures>();;
	
	public Features(){
		String lines[] = ratings.split("\n");
		for(int i = 0; i < lines.length; i++){
			String tokens[] = lines[i].split(",");
			if(!isUserExisting(Long.valueOf(tokens[0]))){
				users.add(new UserFeatures(Long.valueOf(tokens[0])));
			}
			if(!isMovieExisting(Integer.valueOf(tokens[1]))){
				movies.add(new MovieFeatures(Integer.valueOf(tokens[1])));
			}
		}
	}
	
	public void initialize(){
		String lines[] = ratings.split("\n");
		for(int i = 0; i < lines.length; i++){
			String tokens[] = lines[i].split(",");
			if(!isUserExisting(Long.valueOf(tokens[0]))){
				users.add(new UserFeatures(Long.valueOf(tokens[0])));
			}
			if(!isMovieExisting(Integer.valueOf(tokens[1]))){
				movies.add(new MovieFeatures(Integer.valueOf(tokens[1])));
			}
		}
	}
	
	public void setUserFeature(long _user_id, int _index, double _feature){
		int finder;
		if((finder = findUser(_user_id)) >= 0){
			if(_index >=0 && _index < MAX_FEATURES)
				users.get(finder).setFeature(_index, _feature);
		}else{
			users.add(new UserFeatures(_user_id));
		}
	}
	
	public double getUserFeature(long _user_id, int _index){
		int finder;
		if((finder = findUser(_user_id)) >= 0){
			if(_index >=0 && _index < MAX_FEATURES)
				return users.get(finder).getFeature(_index);
			else
				return 0;
		}else{
			users.add(new UserFeatures(_user_id));
			return 0.1;
		}
	}
	
	public void setMovieFeature(int _movie_id, int _index, double _feature){
		int finder;
		if((finder = findMovie(_movie_id)) >= 0){
			if(_index >=0 && _index < MAX_FEATURES)
				movies.get(finder).setFeature(_index, _feature);
		}else{
			movies.add(new MovieFeatures(_movie_id));
		}
	}
	
	
	public double getMovieFeature(int _movie_id, int _index){
		int finder;
		if((finder = findMovie(_movie_id)) >= 0){
			if(_index >=0 && _index < MAX_FEATURES)
				return movies.get(finder).getFeature(_index);
			else
				return 0;
		}else{
			movies.add(new MovieFeatures(_movie_id));
			return 0.1;
		}
	}
	
	public int findUser(long _user_id){
		for(int i = 0; i < users.size(); i++){
			if(users.get(i).user_id == _user_id){
				return i;
			}
		}
		return -1;
	}
	
	public int findMovie(int _movie_id){
		for(int i = 0; i < movies.size(); i++){
			if(movies.get(i).movie_id == _movie_id){
				return i;
			}
		}
		return -1;
	}
	
	public boolean isUserExisting(long _user_id){
		for(int i = 0; i < users.size(); i++){
			if(users.get(i).user_id == _user_id){
				return true;
			}
		}
		return false;
	}
	
	public boolean isMovieExisting(int _movie_id){
		for(int i = 0; i < movies.size(); i++){
			if(movies.get(i).movie_id == _movie_id){
				return true;
			}
		}
		return false;
	}
	
	public String toString(){
		String strRtn = "";
		
		/* User part */
		strRtn += "Users:\n";
		for(int i = 0; i < users.size(); i++){
			for(int j = 0; j < MAX_FEATURES; j++)
				strRtn += String.valueOf(users.get(i).getFeature(j)) + " ";
			strRtn  =strRtn.substring(0, strRtn.length() - 2);
			strRtn += "\n";
		}
		
		/* Movie part */
		strRtn += "Movies:\n";
		for(int i = 0; i < movies.size(); i++){
			for(int j = 0; j < MAX_FEATURES; j++)
				strRtn += String.valueOf(movies.get(i).getFeature(j)) + " ";
			strRtn  =strRtn.substring(0, strRtn.length() - 2);
			strRtn += "\n";
		}
		
		return strRtn;
	}
	
	
	/**
	 * 
	 * @author jarod
	 *
	 */
	private class UserFeatures{
		private long user_id;
		private double[] features;
		
		UserFeatures(){
			user_id = 0;
			features = null;
		}
		
		UserFeatures(long _user_id){
			user_id = _user_id;
			features = new double[MAX_FEATURES];
			for(int i = 0; i < MAX_FEATURES; i++){
				features[i] = 0.1;
			}
		}
		
		void setFeature(int index, double feat){
			if(index >=0 && index <MAX_FEATURES)
				features[index] = feat;
		}
		
		double getFeature(int index){
			if(index >= 0 && index < MAX_FEATURES)
				return features[index];
			else
				return 0;
		}
	}
	
	/**
	 * 
	 * @author jarod
	 *
	 */
	private class MovieFeatures{
		private int movie_id;
		private double[] features;
		
		MovieFeatures(){
			movie_id = 0;
			features = null;
		}
		
		MovieFeatures(int _movie_id){
			movie_id = _movie_id;
			features = new double[MAX_FEATURES];
			for(int i = 0; i < MAX_FEATURES; i++){
				features[i] = 0.1;
			}
		}
		
		void setFeature(int index, double feat){
			if(index >=0 && index <MAX_FEATURES)
				features[index] = feat;
		}
		
		double getFeature(int index){
			if(index >= 0 && index < MAX_FEATURES)
				return features[index];
			else
				return 0;
		}
	}

}