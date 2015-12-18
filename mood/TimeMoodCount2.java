package Mood;

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
 * 统计每小时发布的内容或者转发微博内容包含特定mood关键字的微博数量
 */
public class TimeMoodCount2 {
    public static class Map extends MapReduceBase implements Mapper<LongWritable,Text, Text, Text> {
        private Text x = new Text();
        private Text y = new Text();
        ArrayList<String> tempList = new ArrayList<String>();
        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:00:00");
        List<Status> statusList = new ArrayList<Status>();
        Status rtStatus = null;
        String endTime = null;
        String time = null;
        String text = null;
        String rtText = null;
        ArrayList<String> motionList = new ArrayList<String>();
        private static String separator = "|#|";
        
        public void configure(JobConf job) 
        {
            Calendar c=Calendar.getInstance(); 
            c.set(2013,01,01,00,00,00);
            endTime = inputFormat.format(c.getTime());
            FileInputStream fis;
            InputStreamReader isr;
            BufferedReader br;
            String line = null;
            try {
              Path[] file = DistributedCache.getLocalCacheFiles(job);
              fis = new FileInputStream(file[0].toString());
            isr = new InputStreamReader(fis,"utf8");
            br = new BufferedReader(isr);
            while((line = br.readLine()) != null)
            {
                motionList.add(line);
            }
          } catch (IOException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
          }
        }
        
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        	String values=value.toString();
        	values=values.replaceAll("\t", "|#|");
        	
        	String[] info=values.split("\\|"+"\\#"+"\\|");
        	
        	//原创
        	/*if(info.length==4)
       	    {
       		 text=info[2];
       		 
       		 for(String motion:motionList)
             {
       			if(text.contains(motion))
                 {
       			 if(info[3].contains(" ")){
					   info[3]=info[3].substring(0,info[3].indexOf(" "));
				   }
                    x.set(info[1]+"\t"+info[3]+"\t"+motion);
                    y.set(1+"");
                    output.collect(x, y);
                  }
                }
            }*/
        	
        
        	if(info.length==5)
        	 {
        		 text=info[2];
        		 rtText=info[4];
        		 
        		 
        			 /*if(text.contains("// @"))
                     {
        				 text = text.replaceAll("// @", "//@");
                     }
        			 if(text.contains("//@"))
                     {
                         text = text.substring(0,text.indexOf("//@"));
                     }*/
        			 text+=rtText;
        			 for(String motion:motionList)
                     {
        			  if(text.contains(motion))
                      {
        				   if(info[3].contains(" ")){
        					   info[3]=info[3].substring(0,info[3].indexOf(" "));
        				   }
                         
        				   x.set(info[1]+"\t"+info[3]+"\t"+motion);
                           y.set(1+"");
                           output.collect(x, y);
                      }
                    }
             }
        	 
            }
      }

      public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> 
      {
          int count = 0;
          public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
          {
              count = 0;
              while (values.hasNext()) 
              {
                  //count+= Integer.parseInt(values.next().toString());
                  count++;
                  values.next();
              }
              output.collect(key, new Text(count+""));
          }
      }
      public static void main(String[] args) throws Exception {
          JobConf conf = new JobConf(TimeMoodCount.class);
          conf.setJobName("Motion All Retweet2");

          DistributedCache.addCacheFile(new URI("/home/haixinma/weibo/motion.txt#motion.txt.txt"),conf);
          conf.setOutputKeyClass(Text.class);
          conf.setOutputValueClass(Text.class);
          conf.setMapperClass(Map.class);
          conf.setReducerClass(Reduce.class);
          conf.setInputFormat(TextInputFormat.class);
          conf.setOutputFormat(TextOutputFormat.class);
          //conf.setInt("mapred.min.split.size", 1000000000);
          conf.setInt("mapred.min.split.size", 268435456);
          conf.setInt("mapred.task.timeout", 2400000);
          //conf.setNumMapTasks(20000);
          conf.setNumReduceTasks(50);
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
