###############################################
##   Author : Nikhila Chireddy
##   Date : 03-23-2017
###############################################


import java.io.BufferedReader;

//import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
//import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class MainClass {
	public static HashMap<String,Double> wordIDF=new HashMap<String,Double>();
	public static String newAAV = null;
	public static String count = null;
public static void main(String[] args) throws IOException, ClassNotFoundException,InterruptedException {
	
	
	Configuration conf =new Configuration();
	
	if(args.length == 3)
	{
		Job job=Job.getInstance(conf);
		job.setJarByClass(MainClass.class);
		job.setMapperClass(UnknownUnigramMapper.class);
		job.setReducerClass(UnknownUnigramReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"/Profile1"));
		job.waitForCompletion(true);
		
		conf = new Configuration();
		
		Job job1=Job.getInstance(conf);
		job1.setMapperClass(TFMapper.class);
		job1.setReducerClass(TFReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job1, new Path(args[1]+"/Profile1"));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]+"/TFUnknown1"));
		job1.waitForCompletion(true);
		
		conf = new Configuration();
		
		Job job2=Job.getInstance(conf);
		
		job2.setMapperClass(TFIDFMapper.class);
		job2.setReducerClass(TFIDFReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		MultipleInputs.addInputPath(job2,new Path(args[1]+"/TFUnknown1"), TextInputFormat.class);
		MultipleInputs.addInputPath(job2,new Path(args[1]+"/IDF"), TextInputFormat.class);
		FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/TFIDFUnknown1"));
		job2.waitForCompletion(true);
		
		String str = "hdfs://annapolis:30241/outputBigData1/IDF/part-r-00000";
		Path pt=new Path(str);
	    FileSystem fs = FileSystem.get(conf);
	    BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
	    String line;
	    line=br.readLine();
	    while(line != null){
	            String[] arg = line.split("\t");
	            String word = arg[0];
	            double idf = Double.parseDouble(arg[1]);
	            if(!wordIDF.containsKey(word)){
	            	wordIDF.put(word, idf);
	            }
	            line = br.readLine();
	    }
		
		conf = new Configuration();
		Job job3=Job.getInstance(conf);
		
		job3.setMapperClass(AAVMapper.class);
		job3.setReducerClass(AAVReducer2.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		//MultipleInputs.addInputPath(job2,new Path(args[1]+"/TFIDFUnknown"), TextInputFormat.class);
		//MultipleInputs.addInputPath(job2,new Path(args[1]+"/IDF"), TextInputFormat.class);
		FileInputFormat.setInputPaths(job3, new Path(args[1]+"/TFIDFUnknown1"));
		FileOutputFormat.setOutputPath(job3, new Path(args[1]+"/AAVUnknown1"));
		job3.waitForCompletion(true);
		
		
		str = "hdfs://annapolis:30241/outputBigData1/AAVUnknown1/part-r-00000";
		pt=new Path(str);
	    //FileSystem fs = FileSystem.get(conf);
	    br=new BufferedReader(new InputStreamReader(fs.open(pt)));
	    String[] line1 = br.readLine().split("\t");
	    MainClass.newAAV = line1[1].substring(1, line1[1].length()-1);

	    	
		conf = new Configuration();
		Job job4=Job.getInstance(conf);
		
		job4.setMapperClass(CosineMapper.class);
		job4.setCombinerClass(CosineCombiner.class);
		job4.setReducerClass(CosineReducer.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		job4.setInputFormatClass(TextInputFormat.class);
		job4.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job4, new Path(args[1]+"/AAV"));
		FileOutputFormat.setOutputPath(job4, new Path(args[1]+"/top10List1"));
		
		System.exit(job4.waitForCompletion(true) ? 0 : 1);
		
	}
	else{
	Job job=Job.getInstance(conf);
	job.setJarByClass(MainClass.class);
	job.setMapperClass(Mapper1B.class);
	job.setReducerClass(NGramReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
	FileInputFormat.setInputPaths(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]+"/unigramProfile"));
	
	job.waitForCompletion(true);
	
	conf = new Configuration();
	
	Job job1=Job.getInstance(conf);
	
	job1.setMapperClass(TFMapper.class);
	job1.setReducerClass(TFReducer.class);
	job1.setOutputKeyClass(Text.class);
	job1.setOutputValueClass(Text.class);
	job1.setInputFormatClass(TextInputFormat.class);
	job1.setOutputFormatClass(TextOutputFormat.class);
	FileInputFormat.setInputPaths(job1, new Path(args[1]+"/unigramProfile"));
	FileOutputFormat.setOutputPath(job1, new Path(args[1]+"/TF"));
	job1.waitForCompletion(true);
	
    conf = new Configuration();
	
	Job job2=Job.getInstance(conf);
	
	job2.setMapperClass(MapperAuthCount.class);
	job2.setReducerClass(ReducerAuthCount.class);
	job2.setOutputKeyClass(Text.class);
	job2.setOutputValueClass(Text.class);
	job2.setInputFormatClass(TextInputFormat.class);
	job2.setOutputFormatClass(TextOutputFormat.class);
	FileInputFormat.setInputPaths(job2, new Path(args[1]+"/unigramProfile"));
	FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/AuthorCount"));
	job2.waitForCompletion(true);
	
	conf = new Configuration();
	
	Job job3=Job.getInstance(conf);
	
	job3.setMapperClass(MapperAuthWord.class);
	job3.setReducerClass(ReducerAuthWord.class);
	job3.setOutputKeyClass(Text.class);
	job3.setOutputValueClass(Text.class);
	job3.setInputFormatClass(TextInputFormat.class);
	job3.setOutputFormatClass(TextOutputFormat.class);
	FileInputFormat.setInputPaths(job3, new Path(args[1]+"/unigramProfile"));
	FileOutputFormat.setOutputPath(job3, new Path(args[1]+"/WordAuthorCount"));
	job3.waitForCompletion(true);
	

	
	/*conf = new Configuration();
	String str = "hdfs://annapolis:30241/outputData/AuthorCount/part-r-00000";
	Path pt=new Path(str);
    FileSystem fs = FileSystem.get(conf);
    BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
    String line;
    line=br.readLine();
    if(line != null){
            String[] arg = line.split("\t");
            count = arg[1];
    }
	fs.close();
	
	conf.set("count", count);
	while(true){
		if(!count.isEmpty())
			break;
	}*/
	
	Job job4=Job.getInstance(conf);
	
	job4.setMapperClass(IDFMapper.class);
	job4.setReducerClass(IDFReducer.class);
	job4.setOutputKeyClass(Text.class);
	job4.setOutputValueClass(Text.class);
	job4.setInputFormatClass(TextInputFormat.class);
	job4.setOutputFormatClass(TextOutputFormat.class);
	job4.addCacheFile(new Path("hdfs://annapolis:30241/outputData/AuthorCount/part-r-00000").toUri());
	FileInputFormat.setInputPaths(job4, new Path(args[1]+"/WordAuthorCount"));
	FileOutputFormat.setOutputPath(job4, new Path(args[1]+"/IDF"));
	job4.waitForCompletion(true);
	
	conf = new Configuration();
	
	Job job5=Job.getInstance(conf);
	
	job5.setMapperClass(TFIDFMapper.class);
	job5.setReducerClass(TFIDFReducer.class);
	job5.setOutputKeyClass(Text.class);
	job5.setOutputValueClass(Text.class);
	job5.setInputFormatClass(TextInputFormat.class);
	job5.setOutputFormatClass(TextOutputFormat.class);
	MultipleInputs.addInputPath(job5,new Path(args[1]+"/TF"), TextInputFormat.class);
	MultipleInputs.addInputPath(job5,new Path(args[1]+"/IDF"), TextInputFormat.class);
	
	FileOutputFormat.setOutputPath(job5, new Path(args[1]+"/TFIDF"));
	job5.waitForCompletion(true);
	
	String str = "hdfs://annapolis:30241/outputData/IDF/part-r-00000";
	Path pt=new Path(str);
    FileSystem fs = FileSystem.get(conf);
    BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
    String line;
    line=br.readLine();
    while(line != null){
            String[] arg = line.split("\t");
            String word = arg[0];
            double idf = Double.parseDouble(arg[1]);
            if(!wordIDF.containsKey(word)){
            	wordIDF.put(word, idf);
            }
            line = br.readLine();
    }
    /*int i =0 ;
    for(String s : wordIDF.keySet()){
    	System.out.println(s+"\t"+wordIDF.get(s));
    	i++;
    	if(i == 20)
    		break;
    }*/
	
    conf = new Configuration();
	Job job6=Job.getInstance(conf);
	
	job6.setMapperClass(AAVMapper.class);
	job6.setReducerClass(AAVReducer2.class);
	job6.setOutputKeyClass(Text.class);
	job6.setOutputValueClass(Text.class);
	job6.setInputFormatClass(TextInputFormat.class);
	job6.setOutputFormatClass(TextOutputFormat.class);
	FileInputFormat.setInputPaths(job6, new Path(args[1]+"/TFIDF"));
	FileOutputFormat.setOutputPath(job6, new Path(args[1]+"/AAV"));

	
	System.exit(job6.waitForCompletion(true) ? 0 : 1);
}
}
}