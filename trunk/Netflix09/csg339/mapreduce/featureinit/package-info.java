/**
 * This package contains the initializer for the feature vectors. The 
 * number of features and also the initial value can be found and set
 * in {@link csg339.mapreduce.predlearner.util.Globals} class.
 * 
 * The implementation of this class is based on a simple map/reduce
 * pattern: for each rating from the input training file, two keys, one
 * for user and the other for movie, are extracted and passed to the 
 * reducer. The reducer then just out put the key with a string of the
 * initial value of features.
 */
package csg339.mapreduce.featureinit;