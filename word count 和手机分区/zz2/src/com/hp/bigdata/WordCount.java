package com.hp.bigdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class WordCount {
	public static void main(String[] args) throws Exception {
		//编写提交代码
		//1.创建configuration对象
		Configuration conf = new Configuration();
		//2.创建job对象
		Job job = Job.getInstance(conf);
		//3.设置提交类
		job.setJarByClass(WordCount.class);
		//4.设置map和reduce关系
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		//5.设置map和reduce的输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//6.设置文件的输入和输出路径
		FileInputFormat.setInputPaths(job, new Path("/words.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/output"));
		//7.提交
		job.waitForCompletion(true);
	}
	static class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		/*
		 * KEYIN:首行偏移量
		 * VALUEIN:一行数据
		 * KEYOUT:输出key
		 * VALUEOUT:输出的value
		 * */	
		//重写map方法
			@Override
			protected void map(LongWritable key, Text value,
					org.apache.hadoop.mapreduce.Mapper.Context context)
					throws IOException, InterruptedException {
				// TODO Auto-generated method stub
				//1.读取一行数据
				String line = value.toString();
				//2.通过指定规则截取数据
				String[] words = line.split("");
				//3.初始化valueout 赋值1
				IntWritable ONE = new IntWritable();
				for (String word : words) {
					//写到上下文中
					context.write(new Text(word), ONE);
		 }
	}
}
				//1.map的输出类型是reduce的输入类型
			static class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
				//重写reduce方法
				protected void reduce(Text key, Iterable<IntWritable> ones,
						org.apache.hadoop.mapreduce.Reducer.Context context)
						throws IOException, InterruptedException {
					// TODO Auto-generated method stub
					//1.定义一个空变量的sum
					int sum = 0;
					//2.迭代所有的1相加
					for (IntWritable one : ones) {
						sum ++;
			}
					//3.输出结果到上下文
					context.write(key, new IntWritable(sum));
		}
	}
}
