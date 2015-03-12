# Introduction #

This page shows you how to run the source code.

# Version #

1.0

# System Overview #

This software is an implementation of SVD predicting algorithm on Netflix data running on the Hadoop system.

# System Requirement #

## Operation System ##
Windows, Linux or Mac OS X, with accessible Hadoop system (either locally or remotely) with version 0.19.0 or higher.

## Runtime ##
Java SE 1.6 or higher.

## Others ##
SSH if you are using a remote Hadoop server; we also recommend you to run our code in Eclipse environment.

# Components of the Software #

This software are consisted of three main parts:

(1) Randomizer(csg339.mapreduce.loader): With the input of a file containing ratings in the format of

_**user\_id, movie\_id, rating**_

this part can be used to randomize the input data with randomized order on user id or movie id. The output of it is in the same format as the input file.

(2) Feature Initializer(csg339.mapreduce.featureinit): With the input of a file containing ratings in the format of

_**user\_id, movie\_id, rating**_

it can extract all the user id's and movie id's  and generate a plain file containing the initial feature value on each of the id's. The output of it is a plain file with lines in the following format

_**id    type    feature1    feature2    ...    featureMAX**_

where type is either 1 (id is a movie id) or 0 (id is a user id). The MAX number of features can be pre-defined in the program.

(3) Predictor(csg339.mapreduce.predlearner): This is the main predicting part of the software, using an plain text input file with ratings in the format (user\_id, movie\_id, rating), and also an initial feature vector file containing all the user id's and movie id's in the following format:

_**id    type    feature1    feature2    ...    featureMAX**_

where type is either 1 (id is a movie id) or 0 (id is a user id). The MAX number of features can be pre-defined in the program.

The learner will tune the value in the feature vector file by our SVD algorithm. The algorithm may run several phrases, each time the feature vector file will be tuned based on the file generated from the previous phrase. The final output will be a feature vector file with feature values tuned.

# How to use #

We will provide two alternatives for running our code: one is under the command-line, and the other one is under Eclipse. We recommend you to use the later, since it is easier to catch any exceptions and do modifications in Eclipse, and also since Eclipse is platform-free, you can use it no matter what kind of system you are using.

In the following instruction, we have these assumptions:

Your Hadoop home path: _**${HADOOP\_HOME}=/hadoop/home/**_

Your Hadoop version: _**${HADOOP\_VERSION}**_

Your source code path: _**/code/src/**_

Your Netflix data path: _**/Netflix/Data/**_

Your data on Hadoop system: _**/hadoop/system/data/**_

Here for the data file, it should have the format that

_**user\_id\_1, movie\_id\_1, rating1**_

_**user\_id\_2, movie\_id\_2, rating2**_

_**...**_

You can have several files in this format, but they should be in the same data directory.

Note: You should change the data directory in the source code so that your data on Hadoop system can be found.

And before any activities on your code, you should upload the training data onto the Hadoop file system:

****$ /hadoop/home/bin/hadoop dfs -put /Netflix/Data/** /hadoop/system/data/**

## Command-line setup ##

(0). Update data path in the source code, and generate JAR file:

Go into the csg339.mapreduce.predlearner.util.Globals.java file, change the trainingPath to be your data directory on Hadoop system:

_**public static String trainingPath = "/hadoop/system/data"**_

You can also setup the number of iterations, the initial value of feature and also the number of features.

And then make the JAR file using following commands:

I. Go to the project folder, compile your source code:

_**$ cd /code/src/**_

_**$ mkdir src\_class**_

****$ javac -classpath ${HADOOP\_HOME}/hadoop-${HADOOP\_VERSION}-core.jar;${HADOOP\_HOME}/lib/commons-logging-1.0.4.jar -d src\_class****

II. Generate the JAR file:

_**$ jar -cvf /code/src/netflix.jar -C /code/src/src\_class/ .**_

(1) Do the learning:

_**$ /hadoop/home/bin/hadoop jar /code/src/nextflix.jar csg339.mapreduce.predlearner.system.NetflixAlgorithm**_

4.2 Eclipse setup

(0) If possible, please download the hadoop-eclipse plugin, and copy the plugin folder/jar file into the plugin folder of Eclipse installation directory. The plugin can be found in the latest distribution of Hadoop in {$HADOOP\_HOME}/contrib/eclipse-plugin, or you can find it by searching in Google. We recommand you to use the version from:

http://code.google.com/edu/parallel/tools/hadoopvm/hadoop-eclipse-plugin.jar

(1) In Eclipse, select "File..." > "Import...". Select "File System" in the pop-up window. Find the directory containing the source file, then click "Finish"

(2) You then need to specify the hadoop libraries and also the Java SDK. Right click the project in the Package view tab, select "Properties". Find the "Java Build Path" options, then add {$HADOOP\_HOME}/hadoop-{$HADOOP\_VERSION}-core.jar and {$HADOOP\_HOME}/lib/commons-logging-1.0.4.jar into the libraries. Then add the JRE System Library (recommend to use JRE1.6.0 or higher), if necessary.

(3) Set the training set path, number of iteration, and number of features in csg339.mapreduce.predlearner.util.Globals.java.

(4) Run the project:

I. If you have installed the hadoop-eclipse plugin following step (0), then you can open the MapReduce perspective. Add a new MapReduce server by clicking the blue elephant in the "MapReduce Servers" tab. You may also need to set the hadoop home directory in "Window" -> "Preference" -> "Hadoop Home Directory". Then you can run the project by right-click on NetflixAlgorithm.java and selecting "Run As" -> "Run on Hadoop".

II. If you have not installed the hadoop-eclipse plugin, or your plugin doesn't work well, you can make the jar file by yourself and run it in the command-line. Just right click the project and select "Export...", select "jar" file. Make sure that you have selected all the source java files under the project package by, then press "Finish". After finishing the export, you can run the project in the command-line with this command:

$ /hadoop/home/bin/hadoop jar /code/src/nextflix.jar csg339.mapreduce.predlearner.system.NetflixAlgorithm

Have fun and win the big prize! Thanks!