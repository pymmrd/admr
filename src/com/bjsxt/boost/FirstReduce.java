package com.bjsxt.boost;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * TF 统计为一个文件，输出形式（关键字_用户ID）：
 * 	九阳-001  2
 *  九阳-002  3
 *  
 *  N 统计总数输出为一个文件，输出形式；
 *  count   19245
 * 	
 */
public class FirstReduce extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1,
			 Context arg2) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable i:arg1) {
			sum = sum + i.get();
		}
		if (arg0.equals(new Text("count"))) {
			System.out.println(arg0.toString() +"______________" + sum);
		}
		arg2.write(arg0, new IntWritable(sum));
	}
	
	
}
