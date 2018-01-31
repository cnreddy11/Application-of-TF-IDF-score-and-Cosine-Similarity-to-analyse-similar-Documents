###############################################
##   Author : Nikhila Chireddy
##   Date : 03-23-2017
###############################################


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class MapperAuthCount extends Mapper<LongWritable, Text, Text, Text>{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String[] lines = value.toString().split("\n");
		String str = "XXXXXXXXXX";
		for(String l : lines)
		{
		    String[] args= l.split("\t");
		    String author = args[1];
		    context.write(new Text(str), new Text(author));
		}
				
		
		
	}
}