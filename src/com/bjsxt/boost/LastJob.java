package com.bjsxt.boost;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class LastJob {

	public static void main(String[] args) {
		Configuration config = new Configuration();
		config.set("yarn.resourcemanager.hostname", "hadoop1");
		try {
			//JobConf job = new JobConf(config);
			Job job = new Job(config);
			job.setJarByClass(LastJob.class);
			job.setJobName("weibo3");
			//DistributedCache.addCacheFile(uri, conf);
			//2.5
			job.addCacheFile(new Path("/usr/weibo/output1/part-r-00003").toUri());
			job.addCacheFile(new Path("/usr/weibo/output2/part-r-00000").toUri());
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setMapperClass(LastMapper.class);
			job.setReducerClass(LastReduce.class);
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
}
