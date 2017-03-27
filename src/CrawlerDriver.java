import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;

public class CrawlerDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitcode = ToolRunner.run(new Configuration(),new CrawlerDriver(),args);
		System.exit(exitcode);
		
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf,"My Job");
		job.setJarByClass(CrawlerDriver.class);
		job.setJobName("Web Crawler");
		DistributedCache.addFileToClassPath(new Path("/home/vipul/workspace/BasicWebCrawler/bin/jsoup-1.8.2.jar"),conf);
		
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));

		job.setMapperClass(CrawlerMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(CrawlerReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true)?0:1 ; 
	}

}
