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
 * 描述：统计转发数>100的转发微博的信息
 * 
 * 输入：微博集合
 * 输出：转发微博MID  转发微博作者name|#|转发微博作者UID|#|转发微博创建日期|#|转发微博内容|#|转发转发微博的微博总数
 */
public class Backbone {
	
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
        ArrayList<String> keywordList = new ArrayList<String>();
        private final String separator = "|#|";
        
        public void configure(JobConf job) {
            Calendar c=Calendar.getInstance(); 
            c.set(2013,01,01,00,00,00);
            endTime = inputFormat.format(c.getTime());
        }
        
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        	//输出：转发微博MID  微博MID|#|转发微博作者name|#|转发微博作者UID|#|转发微博创建日期|#|转发微博内容
       
            try {
                statusList.clear();
                if(value.toString().startsWith("{")) {
                    StatusWapper wapper = Status.constructWapperStatus(IOTool.removeEol(value.toString()));
                    if(wapper!=null) {
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
            
                if(statusList == null) {
                    return;
                }
                for(Status status : statusList) {
                    time = inputFormat.format(status.getCreatedAt());
                    if(time.compareTo(endTime)>0 || status.getUser()==null || status.getCreatedAt() == null)
                        continue ;
                    text = status.getText();
                    if(status.getRetweetedStatus()!=null) {
                        rtStatus = status.getRetweetedStatus();
                        rtText = rtStatus.getText();
                        rtMid = rtStatus.getMid();
                        if(rtStatus.getUser()!=null && rtStatus.getCreatedAt() !=null) {
                        	x.set(rtMid);
                            y.set(status.getMid()+separator+rtStatus.getUser().getName()+separator+rtStatus.getUser().getId()
                            		+separator+inputFormat.format(rtStatus.getCreatedAt())+separator+rtStatus.getText());
                            output.collect(x, y); 
                        }
                     }
                }
            
            }
        
      }
    
      public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>  {

          private final String separator = "|#|";
          ArrayList<String> tempList = new ArrayList<String>();
          ArrayList<String> identifier = new ArrayList<String>();
          ArrayList<String> rtEdgeList = new ArrayList<String>();
          public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
          //输入：转发微博MID  微博MID|#|转发微博作者name|#|转发微博作者UID|#|转发微博创建日期|#|转发微博内容
          //输出：转发微博MID  转发微博作者name|#|转发微博作者UID|#|转发微博创建日期|#|转发微博内容|#|转发转发微博的微博总数
        	  
              while (values.hasNext()) {
                  String s=values.next().toString();
                  String[] info=s.split("\\|#\\|");
                  if(!identifier.contains(info[0])){
                      identifier.add(info[0]);
                      tempList.add(s);
                  }
              }
              if(tempList.size()>=100) {
                  Collections.sort(tempList);
                  for(String temp:tempList) {
                      String[] list = temp.split("\\|#\\|");
                      if(list.length>=5) {
                          output.collect(key,new Text(list[1]+this.separator+list[2]+this.separator+list[3]+
                                  this.separator+list[4]+this.separator+tempList.size()));
                          break;
                      }
                  }
              }
              tempList.clear();
              identifier.clear();
             
          }
          
      }
      
      public static void main(String[] args) throws Exception {
          JobConf conf = new JobConf(Backbone.class);
          conf.setJobName("Who post hot tweet");
          conf.setOutputKeyClass(Text.class);
          conf.setOutputValueClass(Text.class);
          conf.setMapperClass(Map.class);
          conf.setReducerClass(Reduce.class);
          conf.setInputFormat(TextInputFormat.class);
          conf.setOutputFormat(TextOutputFormat.class);
          conf.setInt("mapred.min.split.size", 268435456);
          conf.setInt("mapred.task.timeout", 2400000);
          conf.setNumReduceTasks(50);
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

