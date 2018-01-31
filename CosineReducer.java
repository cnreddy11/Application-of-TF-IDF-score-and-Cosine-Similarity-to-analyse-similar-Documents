###############################################
##   Author : Nikhila Chireddy
##   Date : 03-23-2017
###############################################


import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CosineReducer extends Reducer<Text,Text,Text,Text>{

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		HashMap<String, Double> unsortedMap = new HashMap<String, Double>();
        for (Text l : values) {
        	String[] args = l.toString().split("\t");
        	if(!unsortedMap.containsKey(args[0])){
        		unsortedMap.put(args[0], Double.parseDouble(args[1]));
        		
        	}	
        }
        Map<String, Double> sortedMap = unsortedMap.entrySet().stream().sorted(Entry.comparingByValue())
        		.collect(Collectors.toMap(Entry::getKey, Entry::getValue,(e1, e2) -> e1, LinkedHashMap::new));
        
        int i = 0;
        for(String s : sortedMap.keySet()){
        	context.write(key, new Text(s));
        	i++;
        	if(i == 10)
        		break;
        }
        	
        }    
        		
}