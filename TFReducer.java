/*
   Author : Nikhila Chireddy
   Date : 03-23-2017
*/


import java.io.IOException;
import java.util.HashMap;


//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TFReducer extends Reducer<Text,Text,Text,Text>{

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
	
		HashMap<String,Double> hash=new HashMap<String,Double>();
		double maxcount = 0.0d;
		//String[] lines = values.toString().split("\n");
		for(Text l : values){
			String[] args = l.toString().split("\t");
			
			String word = args[0];
			String count = args[1];
			
			double ct=Double.parseDouble(count);
			if (!hash.containsKey(word)) {
            	hash.put(word,ct);
			}
			if(ct > maxcount)
				maxcount = ct;
		}
        for(String keyval : hash.keySet()){
        	
        	double tf = (double) (0.5 + 0.5*(hash.get(keyval)/maxcount));
        	
        	context.write(key, new Text(keyval + "\t" + Double.toString(tf)));
        }    
        

	}
		
}
