/* 
Outside source code used in completing this code:
- https://blog.csdn.net/weixin_44630798/article/details/88922070
- http://www.vogella.com/tutorials/JavaRegularExpressions/article.html 
- https://hadoop.apache.org/docs/stable2/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Example:_WordCount_v1.0
- https://www.linkedin.com/pulse/wordcounter-hadoop-windows-practical-shubham-kumar-gupta/
- https://gist.github.com/tuttlem/8707556
- https://services.cs.rutgers.edu/mapreduce/WordCount.java
- https://ssaik.blogspot.com/2016/02/hadoop-word-count-step-by-step-execution.html
- https://wiki.hpc.odu.edu/en/Cluster/Hadoop
- http://andreaiacono.blogspot.com/2014/02/mapreduce-job-explained.html
- https://hpc.wiki.utwente.nl/hadoop:quick_start
- https://stackoverflow.com/questions/37506902/map-reduce-program-throwing-exception-ioexception-type-mismatch-in-key-from-map
- https://blog.fearcat.in/a?ID=01550-53534ed2-1b1f-4971-99ef-329a38d13143
- https://github.com/SamFeig
- https://stackoverflow.com/questions/49124302/hadoop-mapper-parameters-meaning
*/

import java.io.IOException;                                         // Printing input/output
import java.util.StringTokenizer;                                   // Performing string operations

import org.apache.hadoop.conf.Configuration;                        // Hadoop managing mapers etc.
import org.apache.hadoop.fs.Path;                                   // Hadoop hdfs dfs
import org.apache.hadoop.io.IntWritable;                            // Hadoop data type prep for IntWritable
import org.apache.hadoop.io.Text;                                   // Hadoop Processing text
import org.apache.hadoop.mapreduce.Job;                             // Hadoop launching and  managing jobs
import org.apache.hadoop.mapreduce.Mapper;                          // Hadoop defining mappers
import org.apache.hadoop.mapreduce.Reducer;                         // Hadoop defining reducers
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;       // Hadoop managing file imports
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;     // Hadoop managing file exports 

import java.util.regex.Matcher;                                     // REGEX (Regular Expretion) matching to match sections from input into groups
import java.util.regex.Pattern;                                     // REGEX (Regular Expretion) setting the matching pattern criteria

import java.util.Map;                                               // Loads the hash mapping library
import java.util.TreeMap;                                           // 

public class URLCount {                                             // Declaration of new class URLcount

  public static class TokenizerMapper                               // Declaration of TokenizerMapper Hadoop class
       extends Mapper<Object, Text, Text, IntWritable>{             // Assigning that TokenizerMapper blongs to a Mapper class and parameters

    private final static IntWritable one = new IntWritable(1);      // Intializing and assigning variable one a value of int(1) through the Hadoop IntWritable class
    private Text word = new Text();                                 // Assigning variable word value of str() through the Hadoop Text class 

    public void map(Object key, Text value, Context context         
                    ) throws IOException, InterruptedException {    // Declaration of function map(), its parameters, and handeling exceptions
	  Pattern pattern = Pattern.compile("href=\".*\"");               // Declaring a Java Pattern class variable pattern and setting the search operation to look for strings that start with href and follows with whatever
	  StringTokenizer itr = new StringTokenizer(value.toString());    // Using StringTokenizer class itr to first section the input value from the variable Value (new line by default, maybe?), and then converts the sections from Hadoop text class into a Java String type 
	  while (itr.hasMoreTokens()) {                                   // Continuosly checking for more tokens using the StringTokenizer class member function
		  Matcher matcher = pattern.matcher(itr.nextToken());           // Declaring a Matcher Java class matcher and setting it to iterativly check the next token until done
		  while (matcher.find()) {                                      // Setting a rule to an execute an operation as long as there are matches present in matcher 
              String match = matcher.group();                       // Declaring a String Java type match to group the section/groups we identified and extracted from StringTokenizer
                word.set(match.substring(6, match.length()-1));     // Setting the value of Hadoop Text class word by slicing out the first 6 chars (herf=") and last char (") from the value of match
			  context.write(word, one);                                   // Using Java class context to gather and group variables of Hadoop class as (word: Text, one: IntWritable)
		  }                                                             // Ending while(matcher.find()) declaration scope
	  }                                                               // Ending while(itr.hasMoreTokens()) declaration scope
    }                                                               // Ending map(Object key, Text value, Context context) function declaration scope
  }                                                                 // Ending TokenizerMapper class declaration scope 

  public static class IntSumReducer                                 // Declaration of IntSumReducer Hadoop class 
       extends Reducer<Text,IntWritable,Text,IntWritable> {         // Assigning that IntSumReducer blongs to a Reducer class and parameters 
    private IntWritable result = new IntWritable();                 // Intializing the variable result a value of Hadoop IntWritable class 

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException { // Declaration of function reduce(), its parameters, and handeling exceptions 
      int sum = 0;                                                  // Declaring int type variable sum and intializing it to 0
      for (IntWritable val : values) {                              // Checking each element in IntWritable class values as IntWritable class val
        sum += val.get();                                           // Fetching and adding the current value of the IntWritable class variable val to the value of the int type variable sum
      }                                                             // Ending for(IntWritable val : values) loop declaration scope
      result.set(sum);                                              // Assiging the value of the IntWritable type variable result to = the int type variable sum
        if(sum > 5) {                                               // Checking if the int type variable sum has a value > 5
	      context.write(key, result);                                 // Using Java class context to gather and group variables of Hadoop type as (key: object, result: IntWritable) 
        }                                                           // Ending if(sum > 5) conditional declaration scope
    }                                                               // Ending reduce(Text key, Iterable<IntWritable> values, Context context) function declaration scope
  }                                                                 // Ending IntSumReducer class declaration scope
      
      public static class IntSumCombiner                            // Declaration of IntSumCombiner Hadoop class  
       extends Reducer<Text,IntWritable,Text,IntWritable> {         // Assigning that IntSumCombiner blongs to a Reducer class and parameters  
    private IntWritable result = new IntWritable();                 // Intializing the variable result a value of Hadoop IntWritable class  

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException { // Declaration of function reduce(), its parameters, and handeling exceptions  
      int sum = 0;                                                  // Declaring int type variable sum and intializing it to 0
      for (IntWritable val : values) {                              // Checking each element in IntWritable class values as IntWritable class val  
        sum += val.get();                                           // Fetching and adding the current value of the IntWritable class variable val to the value of the int type variable sum 
      }                                                             // Ending for (IntWritable val : values) loop declaration scope
      result.set(sum);                                              // Assiging the value of the IntWritable type variable result to = the int type variable sum 
	  context.write(key, result);                                     // Using Java class context to gather and group variables of Hadoop type as (key: object, result: IntWritable) 
    }                                                               // Ending reduce(Text key, Iterable<IntWritable> values, Context context) function declaration scope
  }                                                                 // Ending IntSumCombiner class declaration scope

  public static void main(String[] args) throws Exception {         // Java main function configuration and parameters setting
    Configuration conf = new Configuration();                       // Intializing variable conf a value Hadoop Configuration class
    Job job = Job.getInstance(conf, "URLCount");                    // Intializing variable job a value Hadoop Job class using the member function getInstance(conf, "URLCount") to create a .jar
    job.setJarByClass(URLCount.class);                              // Assigning the .jar file to be a URLcount declared class
    job.setMapperClass(TokenizerMapper.class);                      // Assigning the Mapper to be a Hadoop TokenizerMapper class 
    job.setCombinerClass(IntSumCombiner.class);                     // Assigning the Combiner to be a IntSumCombiner class
    job.setReducerClass(IntSumReducer.class);                       // Assigning the Reducer to be a IntSumReducer class
    job.setOutputKeyClass(Text.class);                              // Assigning the output of key to be of Hadoop class Text
    job.setOutputValueClass(IntWritable.class);                     // Assigning the output of vlaue to be of Hadoop class IntWritable
    FileInputFormat.addInputPath(job, new Path(args[0]));           // Java configuration and parameters setting for command line input processing
    FileOutputFormat.setOutputPath(job, new Path(args[1]));         // Java configuration and parameters setting for command line output processing 
    System.exit(job.waitForCompletion(true) ? 0 : 1);               // Checking if all operations have been completed to exit program
  }                                                                 // Ending main function declaration scope
}                                                                   // Ending URLcount declaration scope