/*
   Author : Nikhila Chireddy
   Date : 03-23-2017
*/

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class UnknownUnigramMapper extends Mapper<LongWritable, Text, Text, Text>{
	public static long unknownMaxWord = 0;
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String[] lines = value.toString().split("\n");
		for(String l: lines){
			l = l.replaceAll("[^A-Za-z0-9 ]", "").trim();
			String[] args= l.split(" ");
			String dummy = "XXXXXXXXXX";
			for(String s : args){
				context.write(new Text(s),new Text(dummy));
			}
			
		}
		
				
	}
}
