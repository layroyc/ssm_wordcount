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
		//��д�ύ����
		//1.����configuration����
		Configuration conf = new Configuration();
		//2.����job����
		Job job = Job.getInstance(conf);
		//3.�����ύ��
		job.setJarByClass(WordCount.class);
		//4.����map��reduce��ϵ
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		//5.����map��reduce���������
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//6.�����ļ�����������·��
		FileInputFormat.setInputPaths(job, new Path("/words.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/output"));
		//7.�ύ
		job.waitForCompletion(true);
	}
	static class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		/*
		 * KEYIN:����ƫ����
		 * VALUEIN:һ������
		 * KEYOUT:���key
		 * VALUEOUT:�����value
		 * */	
		//��дmap����
			@Override
			protected void map(LongWritable key, Text value,
					org.apache.hadoop.mapreduce.Mapper.Context context)
					throws IOException, InterruptedException {
				// TODO Auto-generated method stub
				//1.��ȡһ������
				String line = value.toString();
				//2.ͨ��ָ�������ȡ����
				String[] words = line.split("");
				//3.��ʼ��valueout ��ֵ1
				IntWritable ONE = new IntWritable();
				for (String word : words) {
					//д����������
					context.write(new Text(word), ONE);
		 }
	}
}
				//1.map�����������reduce����������
			static class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
				//��дreduce����
				protected void reduce(Text key, Iterable<IntWritable> ones,
						org.apache.hadoop.mapreduce.Reducer.Context context)
						throws IOException, InterruptedException {
					// TODO Auto-generated method stub
					//1.����һ���ձ�����sum
					int sum = 0;
					//2.�������е�1���
					for (IntWritable one : ones) {
						sum ++;
			}
					//3.��������������
					context.write(key, new IntWritable(sum));
		}
	}
}
