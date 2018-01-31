/*
   Author : Nikhila Chireddy
   Date : 03-23-2017
*/


import java.io.IOException;

//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IDFReducer extends Reducer<Text,Text,Text,Text>{

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text l : values) {
        	String[] args = l.toString().split("\t");
        	double n = Double.parseDouble(args[0]);
        	double N = Double.parseDouble(args[1]);
        	double idf = (double) Math.log10(N/n);
        	context.write(key, new Text(Double.toString(idf)));
        }
        	
        }    
        		
}
