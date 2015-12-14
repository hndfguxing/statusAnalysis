package backbone;

import Util.IOTool;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import weibo4j.WeiboException;
import weibo4j.model.Status;
import weibo4j.model.StatusWapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 描述：筛选出北京的用户
 * 
 * 输出：用户UID  北京
 */
public class GetBeijingUser {
	
    public static class Map extends MapReduceBase implements Mapper<LongWritable,Text, Text, Text> {
    	
        private Text x = new Text();
        private Text y = new Text();
        ArrayList<String> tempList = new ArrayList<String>();
        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        List<Status> statusList = new ArrayList<Status>();
        Status rtStatus = null;
        String endTime = null;
        String time = null;
        String text = null;
        String rtText = null;
        String rtMid = "";
        Set<String> uidSet = new HashSet<String>();
        String profileImageUrl = null;
        String location = null;
        String getCreatedAt = null;
        String record=null;
        int retweetNum=0;
    	int original=0;
    	int maxFollower=0;
    	String rtLocation = null;
    	String rtRecord = null;
    	Set<String> failedUnameSet = new HashSet<String>();
        
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        	
            try {
            	location=null;
                statusList.clear();
             
                if(value.toString().startsWith("{")) {
                    StatusWapper wapper = Status.constructWapperStatus(IOTool.removeEol(value.toString()));
                    if(wapper!=null)
                    {
                        statusList = wapper.getStatuses();
                    }
                }
                if(value.toString().startsWith("[")) {
                    statusList = Status.constructStatuses(IOTool.removeEol(value.toString()));
                }
                } catch (WeiboException e) {
                    e.printStackTrace();
                } catch (weibo4j.model.WeiboException e) {
                    e.printStackTrace();
                }
            
                if(statusList == null || statusList.size() ==0) {
                    return;
                }
               
                for(Status status : statusList) {
                    if(status.getUser() == null) {
                        continue;
                    }
                	if(status.getUser().getLocation()!=null){
                		location=status.getUser().getLocation();
                    }
                	else{
                		location="";
                	}
                	if(location.contains(" ")){
                		location=location.substring(0,location.indexOf(" "));
                	}
                	if(location.equals("北京")){
                		x.set(status.getUser().getId());
                        y.set(location);
                        output.collect(x, y);
                	}
                }
               
            }
        
      }

      public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
          public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException  {
              while (values.hasNext()) {
            	  String s=values.next().toString();
            	  output.collect(key, new Text(s));
            	  break;
              } 
          }
      }
      
      public static void main(String[] args) throws Exception {
          JobConf conf = new JobConf(GetUserProfile.class);
          conf.setJobName("Extract beijing User Info");
          conf.setOutputKeyClass(Text.class);
          conf.setOutputValueClass(Text.class);
          conf.setMapperClass(Map.class);
          conf.setReducerClass(Reduce.class);
          conf.setInputFormat(TextInputFormat.class);
          conf.setOutputFormat(TextOutputFormat.class);
          conf.setInt("mapred.min.split.size", 268435456);
          conf.setInt("mapred.task.timeout", 2400000);
          conf.setNumReduceTasks(100);
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

