/*
   Author : Nikhila Chireddy
   Date : 03-23-2017
*/


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class CosineMapper extends Mapper<LongWritable, Text, Text, Text>{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String[] lines = value.toString().split("\n");
		double numerator=0;
		double sqrt1 = 0;
		double sqrt2 = 0;
		for(String l : lines)
		{
		    String[] args= l.split("\t");
		    String author = args[0];
		    String[] aavs = args[1].substring(1, args[1].length()-1).split(",");
		    String[] aavsUnknown = MainClass.newAAV.split(",");
		    for(int i=0;i<aavs.length;i++)
		    {
		    	double d1=Double.parseDouble(aavs[i].trim());
		    	double d2=Double.parseDouble(aavsUnknown[i].trim());
		    	numerator = numerator + (d1*d2);
		    	sqrt1 = sqrt1 + (d1*d1);
		    	sqrt2 = sqrt2 + (d2*d2);
		    }
		    sqrt1 = Math.sqrt(sqrt1);
		    sqrt2 = Math.sqrt(sqrt2);
		    double cosineSim = numerator /(sqrt1*sqrt2);
		    context.write(new Text("XXXXXXXXXX"), new Text(author+"\t"+Double.toString(cosineSim)));
		}
				
		
		
	}
}
