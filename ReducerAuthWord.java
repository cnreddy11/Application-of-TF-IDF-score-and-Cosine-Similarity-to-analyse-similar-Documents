/*
   Author : Nikhila Chireddy
   Date : 03-23-2017
*/

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerAuthWord extends Reducer<Text,Text,Text,Text>{

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		Set<String> set = new HashSet<String>();
        for (Text value : values) {
        	set.add(value.toString());
        }
        context.write(key, new Text(Long.toString(set.size())));
	}
		
}
