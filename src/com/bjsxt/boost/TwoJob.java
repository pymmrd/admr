package com.bjsxt.boost;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TwoJob {
	public static void main(String[] args) {
		Configuration config = new Configuration();
		config.set("yarn.resourcemanager.hostname", "hadoop1");
		try {
			//JobConf job = new JobConf(config);
			Job job = new Job(config);
			job.setJarByClass(TwoJob.class);
			job.setJobName("weibo2");
		 
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			job.setMapperClass(TwoMapper.class);
			job.setReducerClass(TwoReduce.class);
			
			//mr运行时的输入数据从hdfs的那个目录中获取
			FileInputFormat.addInputPath(job,  new Path("/usr/weibo/output1"));
			FileOutputFormat.setOutputPath(job, new Path("/usr/weibo/output2"));
			
			boolean f = job.waitForCompletion(true);
			if(f) {
				System.out.println("执行job成功");
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
	}

}
