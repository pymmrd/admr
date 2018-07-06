package com.bjsxt.boost;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FirstJob {
	public static void main(String[] args) {
		Configuration config = new Configuration();
		config.set("yarn.resourcemanager.hostname", "hadoop1");
		try {
		//JobConf job = new JobConf(config);
		Job job = new Job(config);
		job.setJarByClass(FirstJob.class);
		job.setJobName("weibo1");
		// 设置map任务的输出key类型，value类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//job.setMapperClass();
		job.setNumReduceTasks(4); //设置Reduce的数量
		job.setMapperClass(FirstMapper.class);
		job.setCombinerClass(FirstReduce.class);
		job.setReducerClass(FirstReduce.class);
		
		//mr运行时的输入数据从hdfs的哪个目录中获取
		FileInputFormat.addInputPath(job, new Path("/usr/weibo/input"));
		FileOutputFormat.setOutputPath(job, new Path("/usr/weibo/output"));
		
		boolean f = job.waitForCompletion(true);
		if(f) {
			System.out.println("执行job成功");
		}
		}catch(Exception e) {
			e.printStackTrace();
		}
	}

}
