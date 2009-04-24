/**
 * The package contains the loader/randomizer of the ratings. It distorts
 * the order of ratings in the input training file in a random way.
 * 
 * This can also be used to generate a smaller data set with the user/movie
 * id no larger than a threshold. These two threshold can be found and set 
 * in {@link csg339.mapreduce.predlearner.util.Globals}.
 * 
 */
package csg339.mapreduce.loader;