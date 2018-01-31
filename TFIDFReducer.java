/*
   Author : Nikhila Chireddy
   Date : 03-23-2017
*/


import java.io.IOException;
import java.util.HashMap;

//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TFIDFReducer extends Reducer<Text,Text,Text,Text>{

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		HashMap<String,String> hash=new HashMap<String,String>();
        double idf = 0,tfIdf=0;
        
		for (Text l : values) {
        	String[] args=l.toString().split("\t");
        	if(args.length == 1)
        		//System.out.println(args[0]);
        		idf = Double.parseDouble(args[0]);
        	else{
        		if (!hash.containsKey(args[0])) {
        		//	System.out.println(args[0]);
                	hash.put(args[0], args[1]);
                }
        	}
        		
        }
		
		for(String keyval : hash.keySet()){
			tfIdf = (double)(Double.parseDouble(hash.get(keyval)))*idf;
        	context.write(key, new Text(keyval + "\t" + Double.toString(tfIdf)));
        }

        	
		
        }    
        

	
		
}
