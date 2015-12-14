package backbone;

import Util.IOTool;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import weibo4j.WeiboException;
import weibo4j.model.Status;
import weibo4j.model.StatusWapper;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 描述:筛选出内容或者转发微博内容包含事件关键字的微博
 * 
 * 输出:微博MID  微博作者ID|#|微博作者name|#|微博内容|#|微博创建日期|#|微博来源|#|
 * 		转发微博MID|#|转发微博作者ID|#|转发微博作者name|#|转发微博内容|#|转发微博创建日期|#|转发微博来源|#|事件
 */
public class GetEventStatus {
	
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
            		keywordList.add(line);
            	}
            } catch (IOException e) {
            	e.printStackTrace();
            }
        }
        
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        	
            try {
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
            
                if(statusList == null) {
                    return;
                }
                for(Status status : statusList) {
                    time = inputFormat.format(status.getCreatedAt());
                    if(time.compareTo(endTime)>0 || status.getUser()==null || status.getCreatedAt() == null) {
                        continue ;
                    }
                    text = status.getText();
                    if(status.getRetweetedStatus()!=null) {
                        if(text.contains("//@")) {
                            text = text.substring(0,text.indexOf("//@"));
                        }
                        rtStatus = status.getRetweetedStatus();
                        rtText = rtStatus.getText();
                        rtMid = rtStatus.getMid();
                    }
                    else {
                        rtText = "";
                    }
                   
                    for(String keyword:keywordList) {
                        String[] list = keyword.split("\t");
                        String find = list[1];                      
                        Pattern p = Pattern.compile(find);
                        Matcher matcher = p.matcher(text);
                        Matcher matcher1 = p.matcher(rtText);
                        if(matcher.find() || matcher1.find()) {
                            x.set(status.getMid());
                            if(status.getRetweetedStatus()!=null && status.getRetweetedStatus().getUser()!=null) {
                                if(rtStatus.getCreatedAt()!=null) {
                                    y.set(status.getUser().getId()+separator+
                                            status.getUser().getName()+separator+
                                            status.getText()+separator+
                                            inputFormat.format(status.getCreatedAt())+separator+
                                            status.getSource()+separator+
                                            rtMid+separator+
                                            rtStatus.getUser().getId()+separator+
                                            rtStatus.getUser().getName()+separator+
                                            rtText+separator+
                                            inputFormat.format(rtStatus.getCreatedAt())+separator+
                                            rtStatus.getSource()+separator+
                                            list[0]);
                                    output.collect(x, y);
                                }
                            }
                            else {
                                y.set(status.getUser().getId()+separator+
                                        status.getUser().getName()+separator+
                                        text+separator+
                                        inputFormat.format(status.getCreatedAt())+separator+
                                        status.getSource()+separator+
                                        list[0]);
                                output.collect(x, y);
                            }
                        }
                    }
                }
                
            }
        
      }

      public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    	  
          public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        	  
              while (values.hasNext()) {
                  output.collect(key, new Text(values.next()));
                  break;
              }
              
          }
          
      }
      
      public static void main(String[] args) throws Exception {
          JobConf conf = new JobConf(GetEventStatus.class);
          conf.setJobName("Event Data");
          DistributedCache.addCacheFile(new URI("/home/haixinma/weibo/political.txt#political.txt"),conf);
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
          Path output = new Path(args[1]);
          FileSystem fs = FileSystem.get(conf);
          if(fs.exists(output)){
            fs.delete(output,true);
          }
          FileOutputFormat.setOutputPath(conf, output);
          JobClient.runJob(conf);
        }

}

