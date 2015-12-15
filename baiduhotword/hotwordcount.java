package baiduhotword;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
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

/*
 * 统计讨论特定事件的微博数  MapReduce程序
 */
public class hotwordcount {

    public static class Map extends MapReduceBase implements Mapper<LongWritable,Text, Text, Text> {
    	
        private Text x = new Text();
        private Text y = new Text();
        ArrayList<String> tempList = new ArrayList<String>();
        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd");
        List<Status> statusList = new ArrayList<Status>();
        Status rtStatus = null;
        String startTime = null;
        String endTime = null;
        String time = null;
        String text = null;
        String rtText = null;
        String rtMid = "";
        String profileImageUrl = null;
        String location = null;
        String description = null;
        java.util.Map<String,List<String>> hotwordlist = new HashMap<String,List<String>>();
        boolean isRetweet = false;
        
        /*
         * 将事件 关键字列表添加到hotwordlist中
         */
        public void configure(JobConf job) 
        {
        	FileInputStream fis1;
            InputStreamReader isr1;
            BufferedReader br1;
            String line1 = null;
            
            try {
            	Path[] file = DistributedCache.getLocalCacheFiles(job);
            	fis1 = new FileInputStream(file[0].toString());
            	isr1 = new InputStreamReader(fis1,"utf8");
                br1 = new BufferedReader(isr1);
                while((line1 = br1.readLine()) != null)
                {
                	String[] info=line1.split("\t");
                	if(!hotwordlist.containsKey(info[0])){
                		List l=new ArrayList();
                		for(int i=1;i<info.length;i++){
                			l.add(info[i]);
                		}
                        hotwordlist.put(info[0],l);
                	}
                	
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
        {
        	String values=value.toString();
            values=values.replaceAll("\t", "|#|");
                	
            String[] info=values.split("\\|"+"\\#"+"\\|");
                	
            if(info.length==4){
            	text=info[2];											//微博内容
                if(hotwordlist.containsKey(info[1])){					//事件  贵州地震
                	List<String> wordlist=hotwordlist.get(info[1]);		//关键词组列表  
                	
                	for(String word:wordlist){							//关键词组  贵州 地震 5.5级
                		String[]subword=word.split(" ");				//关键词
                			  
                	
                 			boolean b=true;
                 			for(String s:subword){
                 				if(!text.contains(s)){
                 					b=false;
                 					break;
                 					}
                 				}
                 			if(b){
                 			 x.set(info[1]+"\t"+word+"\t"+info[3]);		//输出:事件  关键词组  info[3]  微博MID
                       	     y.set(info[0]);							//微博MID
                       	     output.collect(x, y);
                 			}
                       	    
                         }
                	}
                		
                    	
                    
                    
            
                
            
            }
      }
    }
      public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> 
      {
          public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
          {
        	  java.util.Map<String,String> midList = new HashMap<String,String>();
        	  
              while (values.hasNext()) 
              { 
            	  String mid=values.next().toString();
            	  if(!midList.containsKey(mid)){
            		  midList.put(mid,"");
            	  }
                 
              }
              
              output.collect(key, new Text(String.valueOf(midList.size())));
              midList.clear();
          }
      }
      public static void main(String[] args) throws Exception {
          JobConf conf = new JobConf(hotwordcount.class);
          conf.setJobName("zhongkeyuan count");
          DistributedCache.addCacheFile(new URI("/home/haixinma/weibo/baiduhotwordlist_split.txt#baiduhotwordlist_split.txt"),conf);
          conf.setOutputKeyClass(Text.class);
          conf.setOutputValueClass(Text.class);
          conf.setMapperClass(Map.class);
          conf.setReducerClass(Reduce.class);
          conf.setInputFormat(TextInputFormat.class);
          conf.setOutputFormat(TextOutputFormat.class);
          conf.setInt("mapred.min.split.size", 268435456);
          conf.setInt("mapred.task.timeout", 2400000);
          conf.setNumReduceTasks(10);
          FileInputFormat.setInputPaths(conf, new Path(args[0]));
          Path output = new Path(args[1]);
          FileSystem fs = FileSystem.get(conf);
          if(fs.exists(output)){
            fs.delete(output,true);
          }
          FileOutputFormat.setOutputPath(conf, output);
          JobClient.runJob(conf);
        }



}
