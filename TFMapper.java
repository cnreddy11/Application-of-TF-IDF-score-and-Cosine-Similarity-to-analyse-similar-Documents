/*
   Author : Nikhila Chireddy
   Date : 03-23-2017
*/


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class TFMapper extends Mapper<LongWritable, Text, Text, Text>{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String[] lines = value.toString().split("\n");
		for(String l: lines){
			String[] args= l.split("\t");
			String word = args[0];
			String author = args[1];
			String count = args[2];
			String val = word+"\t"+count;
			if(!val.isEmpty() && !val.equals(""))
			context.write(new Text(author), new Text(val));
		}
		
				
	}
}
