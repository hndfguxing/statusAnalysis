package screenStatus;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

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

import weibo4j.WeiboException;
import weibo4j.model.Status;
import weibo4j.model.StatusWapper;
import Util.Tool;

public class GetStatusByKey {
	
    public static class Map extends MapReduceBase implements Mapper<LongWritable,Text, Text, Text> {
    	
        private Text x = new Text();
        private Text y = new Text();
        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd");
        List<Status> statusList = new ArrayList<Status>();
        String text = null;
        ArrayList<String> keywordList = new ArrayList<String>();
        
        
        public void configure(JobConf job) {
        	//添加关键字到keywordList中
        	
        	keywordList.add("铁路");
        	keywordList.add("铁路事故");
        	keywordList.add("触电");
        	keywordList.add("高压触电");
        	keywordList.add("触电事故");
        	keywordList.add("学校事故");
        	
        }
        
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        	//输出：key-关键字  微博创建时间(yyyy-MM-dd)  value-微博MID
        	
        	//解析微博
            try {
                statusList.clear();
                if(value.toString().startsWith("{")) 
                {
                    StatusWapper wapper = Status.constructWapperStatus(Tool.removeEol(value.toString()));
                    if(wapper!=null)
                    {
                        statusList = wapper.getStatuses();
                    }
                    
                }
                if(value.toString().startsWith("["))
                {
                    statusList = Status.constructStatuses(Tool.removeEol(value.toString()));
                }
			} catch (WeiboException e) {
			    e.printStackTrace();
			} catch (weibo4j.model.WeiboException e) {
			    e.printStackTrace();
			}
            
            if(statusList == null)
            {
                return;
            }
            for(Status status : statusList)
            {
            	if(status.getCreatedAt()==null){
            		return;
            	}
                text = status.getText();
               
                for(String keyword:keywordList)
                {
                   if(text.contains(keyword))
                    {
                	   x.set(keyword+"\t"+inputFormat.format(status.getCreatedAt()));
                       y.set(status.getMid());
                       output.collect(x, y);
                        
                    }
                }
            }
            
        }
        
    }

      public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    	  //输入：key-关键字  微博创建时间(yyyy-MM-dd)  value-微博MID
    	  //输出：key-关键字  微博创建时间(yyyy-MM-dd)  value-微博数
    	  
          public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        	  
        	  java.util.Map<String,String> map=new HashMap<String,String>();
        	  int count = 0;
        	  
              while (values.hasNext()) 
              {
            	  String mid=values.next().toString();
            	  if(!map.containsKey(mid)){
                	  count++;
            		  map.put(mid, "");
            	  }
                 
              } 
              
              output.collect(key, new Text(String.valueOf(count)));
              map.clear();
        	 
          }
          
      }
      
      public static void main(String[] args) throws Exception {
    	  
          JobConf conf = new JobConf(GetStatusByKey.class);
          conf.setJobName("Event Data");
          conf.setOutputKeyClass(Text.class);
          conf.setOutputValueClass(Text.class);
          conf.setMapperClass(Map.class);
          conf.setReducerClass(Reduce.class);
          conf.setInputFormat(TextInputFormat.class);
          conf.setOutputFormat(TextOutputFormat.class);
          conf.setInt("mapred.min.split.size", 268435456);
          conf.setInt("mapred.task.timeout", 2400000);
          conf.setNumReduceTasks(1);
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

