import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CrawlerMapper extends Mapper<LongWritable,Text,Text,Text> {
	private Text url = new Text();
	private Text word = new Text();
	
	@Override
	public void map(LongWritable key, Text value, Context context)
	throws IOException,InterruptedException{
		String val = value.toString();
		String[] arr = val.split(" ");
		url.set(arr[0]);
		word.set(arr[1]);
		context.write(url, word);
	}

}
