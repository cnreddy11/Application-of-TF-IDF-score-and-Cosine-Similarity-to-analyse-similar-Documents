/*
   Author : Nikhila Chireddy
   Date : 03-23-2017
*/


import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UnknownUnigramReducer extends Reducer<Text,Text,Text,Text>{

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
	
		long count = 0;
		for(Text l : values){
			if(l.toString().contentEquals("XXXXXXXXXX")){
					count++;
			}
		}    

		context.write(key, new Text("XXXXXXXXXX"+"\t"+Long.toString(count)));

	}
		
}
