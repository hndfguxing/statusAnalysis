package screenStatus;
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

import weibo4j.WeiboException;
import weibo4j.model.Status;
import weibo4j.model.StatusWapper;
import Util.Tool;

public class GetStatusByUid {
	
    public static class Map extends MapReduceBase implements Mapper<LongWritable,Text, Text, Text> {
    	
        private Text x = new Text();
        private Text y = new Text();
        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        List<Status> statusList = new ArrayList<Status>();
        java.util.Map<String,String> keywordList=new HashMap<String,String>();
        
        public void configure(JobConf job) {
        	//添加UID到keywordList中
        	
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
            	  keywordList.put(line,"");
              }
            }catch (IOException e) {
              e.printStackTrace();
            }
            
        }
        
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        	//输出：key-微博MID  value-微博内容
        	
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
        	
        	
                if(statusList == null){
                    return;
                }
                for(Status status : statusList)
                {
                	if(status.getUser()==null||status.getCreatedAt()==null){
                		continue;
                	}
                	if(keywordList.containsKey(status.getUser().getId())){
                		x.set(status.getMid());
                		y.set(status.toString());
                		output.collect(x,y);
                	}
                }
            
        }
        
    }

      public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    	  
          public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        	  
              while (values.hasNext()) 
              {
                  output.collect(key, new Text(values.next()));
                  break;
              }
              
          }
          
      }
      
      public static void main(String[] args) throws Exception {
    	  
          JobConf conf = new JobConf(GetStatusByUid.class);
          conf.setJobName("Event Data");
          DistributedCache.addCacheFile(new URI("/home/zhangqunyan/input/uid_status.txt#uid_status.txt"),conf);
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
