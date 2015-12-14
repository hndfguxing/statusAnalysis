package backbone;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import weibo4j.model.Status;

/**
 * 描述：根据用户name查询用户UID 
 * 
 * 输出：微作者UID 微作者name
 */
public class GetUIDByName {
	
	
	
    public static class Map extends MapReduceBase implements Mapper<LongWritable,Text, Text, Text> {
    	
        private Text x = new Text();
        private Text y = new Text();
        ArrayList<String> tempList = new ArrayList<String>();
        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        List<Status> statusList = new ArrayList<Status>();
        Status rtStatus = null;
        String endTime = null;
        String startTime = null;
        String time = null;
        String text = null;
        String rtText = null;
        String rtMid = "";
        ArrayList<String> keywordList = new ArrayList<String>();
        
        public void configure(JobConf job) {
        	Calendar c=Calendar.getInstance(); 
            c.set(2012,02,01,00,00,00);
            Calendar s=Calendar.getInstance(); 
            s.set(2011,12,01,00,00,00);
            endTime = inputFormat.format(c.getTime());
            startTime = inputFormat.format(s.getTime());
            FileInputStream fis;
            InputStreamReader isr;
            BufferedReader br;
            String line = null;
            try {
            	 Path[] file = DistributedCache.getLocalCacheFiles(job);
            	 fis = new FileInputStream(file[0].toString());
            	 isr = new InputStreamReader(fis,"utf8");
            	 br = new BufferedReader(isr);
            	 while((line = br.readLine()) != null) {
            		 keywordList.add(line);
            	 }
	        } catch (IOException e) {
	        	e.printStackTrace();
	        }
        }
        
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
           
        	if(value.toString()!=null){
	            try{
	            	String info[]=value.toString().split("\t");
	            	String uid=info[0];
	            	String detail[]=info[1].split("\\|#\\|");
	            	String uname=detail[1];
	            	if(keywordList.contains(uname)){
	            		x.set(uid);
	            		y.set(uname);
	            		output.collect(x, y);
	            	}
	            }catch(Exception e){
	            	return;
	           	}
            }     
            
        }
        
	}   
        	  
    	
      public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    	  
          public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        	  while(values.hasNext()){  
        		  output.collect(key, new Text(values.next().toString()));
        		  break;
        	  }
          }
      }
      
      public static void main(String[] args) throws Exception {
          JobConf conf = new JobConf(GetUIDByName.class);
          conf.setJobName("extract username");

          DistributedCache.addCacheFile(new URI("/home/zhangqunyan/input/userid#userid"),conf);
          conf.setOutputKeyClass(Text.class);
          conf.setOutputValueClass(Text.class);
          conf.setMapperClass(Map.class);
          conf.setReducerClass(Reduce.class);
          conf.setInputFormat(TextInputFormat.class);
          conf.setOutputFormat(TextOutputFormat.class);
          conf.setInt("mapred.min.split.size", 268435456);
          conf.setInt("mapred.task.timeout", 2400000);
          conf.setNumReduceTasks(500);
          FileInputFormat.setInputPaths(conf, new Path(args[0]));
          //FileOutputFormat.setOutputPath(conf, new Path(args[1]));
          Path output = new Path(args[1]);
          FileSystem fs = FileSystem.get(conf);
          if(fs.exists(output)){
            fs.delete(output,true);
          }
          FileOutputFormat.setOutputPath(conf, output);
          JobClient.runJob(conf);
        }
    
}

