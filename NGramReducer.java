###############################################
##   Author : Nikhila Chireddy
##   Date : 03-23-2017
###############################################


import java.io.IOException;
import java.util.HashMap;


//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NGramReducer extends Reducer<Text,Text,Text,Text>{

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		HashMap<String,Long> hash=new HashMap<String,Long>();
		long count = 0;
        for (Text value : values) {
        	count = 0;
            if (!hash.containsKey(value.toString())) {
            	hash.put(value.toString(),count+1);
            }else{
            	hash.put(value.toString(), hash.get(value.toString()) + 1);
            }
        }
        for(String keyval : hash.keySet()){
        	context.write(key, new Text(keyval + "\t" + Long.toString(hash.get(keyval))));
        }    
        

	}
		
}