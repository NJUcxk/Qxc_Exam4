package industrycount;

import java.io.IOException;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class IndustryCount {

	  public static class TokenizerMapper extends
	      Mapper<Object, Text, Text, IntWritable> {

	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	      
	    public void map(Object key, Text value, Context context)
	        throws IOException, InterruptedException {
	    	String str = value.toString();
	    	String[] strList = str.split(",");
	    	if(strList[10].equals("industry"))
	    		return;
	    	word.set(strList[10]);
	        context.write(word, one);
	    }
	  }

	  public static class IntSumReducer extends
	      Reducer<Text, IntWritable, Text, IntWritable> {
	    private IntWritable result = new IntWritable();

	    public void reduce(Text key, Iterable<IntWritable> values, Context context)
	        throws IOException, InterruptedException {
	      int sum = 0;
	      for (IntWritable val : values) {
	        sum += val.get();
	      }
	      result.set(sum);
	      context.write(key, result);
	    }
	  }

	  public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String[] otherArgs =
	        new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: wordcount <in> <out>");
	      System.exit(2);
	    }
	    
	    Path tempDir = new Path("wordcount-temp-" + Integer.toString(  
	            new Random().nextInt(Integer.MAX_VALUE))); //����һ����ʱĿ¼
	    
	    Job job = new Job(conf, "industry count");
	    job.setJarByClass(IndustryCount.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, tempDir);//�Ƚ���Ƶͳ�������������д����ʱĿ  
	    //¼��, ��һ��������������ʱĿ¼Ϊ����Ŀ¼��
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);  
	    if(job.waitForCompletion(true))
	    {  
	        Job sortJob = new Job(conf, "sort");
	        //sortJob.setJarByClass(WordCount.class);  

	        FileInputFormat.addInputPath(sortJob, tempDir);  
	        sortJob.setInputFormatClass(SequenceFileInputFormat.class);  

	        /*InverseMapper��hadoop���ṩ��������ʵ��map()֮������ݶԵ�key��value����*/  
	        sortJob.setMapperClass(InverseMapper.class);
	        sortJob.setReducerClass(SimpleReducer.class); 
	        /*�� Reducer �ĸ����޶�Ϊ1, ��������Ľ���ļ�����һ����*/  
	        sortJob.setNumReduceTasks(1);
	        FileOutputFormat.setOutputPath(sortJob, new Path(otherArgs[1])); 

	        
	        sortJob.setOutputKeyClass(IntWritable.class);  
	        sortJob.setOutputValueClass(Text.class);  
	        /*Hadoop Ĭ�϶� IntWritable ���������򣬶�������Ҫ���ǰ��������С� 
	        * �������ʵ����һ�� IntWritableDecreasingComparator ��,�� 
	        * ��ָ��ʹ������Զ���� Comparator ����������е� key (��Ƶ)��������*/  
	        sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);  
	        
	        System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
	    }
	    
	    //FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    FileSystem.get(conf).deleteOnExit(tempDir); 
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	    
	  }
	  
	  private static class IntWritableDecreasingComparator extends IntWritable.Comparator {
	      public int compare(WritableComparable a, WritableComparable b) {
	        return -super.compare(a, b);
	      }
	      
	      public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	          return -super.compare(b1, s1, l1, b2, s2, l2);
	      }   
	  }
	  
	  public static class SimpleReducer extends Reducer <IntWritable,Text,Text,IntWritable>{
	      public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	          for (Text val:values)
	          {
	                  String t = val.toString();
	                  Text WORD = new Text(t);
	                  context.write(WORD, key);
	              }
	          }
	      }
	  
}
