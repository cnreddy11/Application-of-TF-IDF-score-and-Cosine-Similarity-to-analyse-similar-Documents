###############################################
##   Author : Nikhila Chireddy
##   Date : 03-23-2017
###############################################


import java.io.IOException;

import java.util.HashMap;
import java.util.Vector;

//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AAVReducer2 extends Reducer<Text,Text,Text,Text>{
	
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		Vector<Double> aav = new Vector<Double>();
		HashMap<String,Double> localWordIDF=new HashMap<String,Double>();
		for(Text value : values){
			String[] args = value.toString().split("\t");
			String word = args[0];
			double idf = Double.parseDouble(args[1]);
			if(!localWordIDF.containsKey(word))
				localWordIDF.put(word, idf);        	
        }
		for(String s: MainClass.wordIDF.keySet()){
			if(localWordIDF.containsKey(s))
			{
				aav.add(localWordIDF.get(s));
			}
			else
			{
				double val = (MainClass.wordIDF.get(s))*0.5;
				aav.add(val);
			}
		}    
        
        System.out.println("End of author");
        context.write(key, new Text(aav.toString()));
     
	}    	
		
}