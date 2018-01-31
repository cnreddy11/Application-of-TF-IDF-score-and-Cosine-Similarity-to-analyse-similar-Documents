###############################################
##   Author : Nikhila Chireddy
##   Date : 03-23-2017
###############################################


import java.io.IOException;
/*import java.io.InputStreamReader;
import java.net.URI;
import java.io.BufferedReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;*/
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class IDFMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	
	/*String count=null;
	@Override
	public void setup(Context context) throws IOException{
	
		URI[] uri=context.getCacheFiles();
		String path=uri[0].toString();
		Path pt=new Path(path);
		FileSystem fs = FileSystem.get(new Configuration());
		if(fs.exists(pt))
		{
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line;
			line=br.readLine();
			if(line != null){
				String[] arg = line.split("\t");
				count = arg[1];
			}
		}
		else{
			System.out.println("Error");
			System.exit(0);
    }
	}*/
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		
	    
		
	
		String[] lines = value.toString().split("\n");
		
		for(String l : lines)
		{
			String[] args=l.split("\t");
			String dummy = args[1]+"\t"+ MainClass.count;
			context.write(new Text(args[0]), new Text(dummy));
			
			
		}		
		
		
		
	}
}