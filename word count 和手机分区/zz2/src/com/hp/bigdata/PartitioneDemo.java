package com.hp.bigdata;

import java.io.IOException;
import java.security.Key;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.hp.bigdata.WordCount.WordCountMapper;
import com.hp.bigdata.WordCount.WordCountReducer;

public class PartitioneDemo {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//编写提交代码
				//1.创建configuration对象
				Configuration conf = new Configuration();
				//2.创建job对象
				Job job = Job.getInstance(conf);
				//3.设置提交类
				job.setJarByClass(PartitioneDemo.class);
				//4.设置map和reduce关系
				job.setMapperClass(phoneMapper.class);
				job.setReducerClass(phoneReducer.class);
				//5.设置map和reduce的输出类型
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(IntWritable.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(IntWritable.class);
				//6.设置文件的输入和输出路径
				FileInputFormat.setInputPaths(job, new Path("/phone.txt"));
				FileOutputFormat.setOutputPath(job, new Path("/output"));
				//7.提交
				job.waitForCompletion(true);
	}
	static class phoneMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		@Override
		protected void map(LongWritable key, Text value,
				org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			String[] fields = line.split(" ");
			String phonename = fields[0];
			String num = fields[1];
			context.write(new Text(phonename), new LongWritable(Long.parseLong(num)));
			
		}
	}
	static class phoneReducer extends Reducer<Text, LongWritable, Text, NullWritable>{
		protected void reduce(Text phonename, Iterable<LongWritable> arg1,
				org.apache.hadoop.mapreduce.Reducer.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//遍历所有销量相加
			long sum = 0;
			for (LongWritable num : nums){
				sum += Long.valueOf(num.toString());
			}
			String str = phonename.toString()+"/t"+sum;
			context.write(new Text(str), NullWritable.get());
		}
		}
	static class phonePartitioner extends Partitioner<Text, LongWritable>{

		@Override
		public int getPartition(Text arg0, LongWritable value, int arg2) {
			// TODO Auto-generated method stub
			if(key.toString().equals("xiaomi")){
				return 0;
			}else if(key.toString().equals("huawei")){
				return 1;
			}else{
				return 2;
			}
		}
		
	}
	}

