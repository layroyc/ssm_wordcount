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
		//��д�ύ����
				//1.����configuration����
				Configuration conf = new Configuration();
				//2.����job����
				Job job = Job.getInstance(conf);
				//3.�����ύ��
				job.setJarByClass(PartitioneDemo.class);
				//4.����map��reduce��ϵ
				job.setMapperClass(phoneMapper.class);
				job.setReducerClass(phoneReducer.class);
				//5.����map��reduce���������
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(IntWritable.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(IntWritable.class);
				//6.�����ļ�����������·��
				FileInputFormat.setInputPaths(job, new Path("/phone.txt"));
				FileOutputFormat.setOutputPath(job, new Path("/output"));
				//7.�ύ
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
			//���������������
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

