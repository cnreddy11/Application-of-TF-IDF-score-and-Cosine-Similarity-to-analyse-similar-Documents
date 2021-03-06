/*
   Author : Nikhila Chireddy
   Date : 03-23-2017
*/


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class AAVMapper extends Mapper<LongWritable, Text, Text, Text>{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String[] lines = value.toString().split("\n");
		
		for(String l : lines){
			String[] args=l.split("\t");
			
				String dummy = args[0]+"\t"+ args[2];
				context.write(new Text(args[1]), new Text(dummy));
				
		}
		
		
	}
}
