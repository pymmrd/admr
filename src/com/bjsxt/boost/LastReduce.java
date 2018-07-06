package com.bjsxt.boost;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * 第二个job输出结果：
 * 九阳 2223
 * 豆浆 2345
 */
public class LastReduce extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> arg1,
			 Context context) throws IOException, InterruptedException {
		StringBuffer sb = new StringBuffer();
		for(Text i:arg1) {
			sb.append(i.toString());
		}
	}
	

}
