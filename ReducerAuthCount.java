###############################################
##   Author : Nikhila Chireddy
##   Date : 03-23-2017
###############################################


import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerAuthCount extends Reducer<Text,Text,Text,Text>{
   public Set<String> set = new HashSet<String>();
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		HashMap<String,String> hash=new HashMap<String,String>();
		long count = 0;
        for (Text value : values) {
            if (!hash.containsKey(value.toString())) {
            	hash.put(value.toString(),"XXXXXXXXXX");
            	count = count+1;
            	set.add(value.toString());
            }
        }
        MainClass.count = Long.toString(count);
        context.write(key, new Text(Long.toString(count)));
    }    		
}