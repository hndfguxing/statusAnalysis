package HotTopic;

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
 * 统计每个省份每小时里谈论特定事件的微博数
 */

public class EventLocationStatis {
    public static class Map extends MapReduceBase implements Mapper<LongWritable,Text, Text, Text> {
        private Text x = new Text();
        private Text y = new Text();
        ArrayList<String> tempList = new ArrayList<String>();
        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:00:00");
        List<Status> statusList = new ArrayList<Status>();
        String endTime = null;
        String time = null;
        String text = null;
        String rtText = null;
        String location = null;
        ArrayList<String> keywordList = new ArrayList<String>();
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
                  keywordList.add(line);
              }
          } catch (IOException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
          }
        }
        
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            try {
                statusList.clear();
                if(value.toString().startsWith("{")) 
                {
                    StatusWapper wapper = Status.constructWapperStatus(IOTool.removeEol(value.toString()));
                    if(wapper!=null)
                    {
                        statusList = wapper.getStatuses();
                    }
                    
                }
                if(value.toString().startsWith("["))
                {
                    statusList = Status.constructStatuses(IOTool.removeEol(value.toString()));
                }
                } catch (WeiboException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (weibo4j.model.WeiboException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                if(statusList == null)
                {
                    return;
                }
                for(Status status : statusList)
                {
                    time = inputFormat.format(status.getCreatedAt());
                    if(time.compareTo(endTime)>0 || status.getUser()==null || status.getCreatedAt() == null)
                    {
                        continue ;
                    }
                    text = status.getText();
                    if(status.getRetweetedStatus()!=null)
                    {
                        rtText = status.getRetweetedStatus().getText();
                    }else
                    {
                        rtText = "";
                    }
                    /*
                     * 如果是讨论事件的微博，生成记录:
                     * key : event+location+time(location只到省份)
                     * value : uid+content+time+source+repostNum+commentNum+event+rtMid(如果是原创微博，则RTMid为-1)
                     * 如果是原创微博，则生成一条新的微博记录
                     */
                    
                    for(String keyword:keywordList)
                    {
                        String[] list = keyword.split("\t");
                        String find = list[1];                      
                        Pattern p = Pattern.compile(find);
                        Matcher matcher = p.matcher(text);
                        Matcher macher1 = p.matcher(rtText);
                        if(matcher.find() || macher1.find())
                        {
                            if(status.getUser()!= null && status.getUser().getLocation()!=null)
                            {
                                location = status.getUser().getLocation();
                            }else
                            {
                                location = "其他";
                            }
                            /*if(location.contains(" "))
                            {
                                StringTokenizer st= new StringTokenizer(location);
                                location = st.nextToken();
                            } */
                            x.set(list[0]+this.separator+location+this.separator+inputFormat.format(status.getCreatedAt()));
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
                  values.next();
                  ++count;
              }
              output.collect(key, new Text(count+""));
          }
      }
  public static void main(String[] args) throws Exception {
      JobConf conf = new JobConf(EventLocationStatis.class);
      conf.setJobName("Event Location Statis");
      DistributedCache.addCacheFile(new URI("/home/haixinma/weibo/political.txt#political.txt"),conf);
      //DistributedCache.addCacheFile(new URI("/home/haixinma/weibo/motion.txt#motion.txt"),conf);
      
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
