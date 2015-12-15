package Location;

import Util.IOTool;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import weibo4j.WeiboException;
import weibo4j.model.Status;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.*;

/*
 * 统计每个city谈论特定事件的微博数
 */
public class PopularMotion {

    public static class Map extends MapReduceBase implements Mapper<LongWritable,Text, Text, Text> {
        private Text x = new Text();
        private Text y = new Text();
        ArrayList<String> keywordList = new ArrayList<String>();
        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        List<Status> statusList = new ArrayList<Status>();
        String endTime = null;
        String time = null;
        String text = null;
        String location = null;
        String province = null;
        String city = null;
        private static String separator = "|#|";
        
        public void configure(JobConf job) 
        {
            Calendar c=Calendar.getInstance(); 
            c.set(2012,01,01,00,00,00);
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
              statusList = Status.constructStatuses(value.toString());
              if(statusList == null)
              {
                  return;
              }
              for(Status status : statusList)
              {
                  time = inputFormat.format(status.getCreatedAt());
                  if(time.compareTo(endTime)<0)
                  {
                      if(status.getRetweetedStatus()!=null && status.getUser()!=null)
                      {
                          text = status.getText();
                          text = IOTool.removeEol(text);
                          location = status.getUser().getLocation();
                          if(location == null || location.equals(""))
                          {
                              province = null;
                              city = null;
                          }
                          else if(!location.contains(" "))
                          {
                              province = location;
                              city = null;
                          }
                          else
                          {
                              StringTokenizer st= new StringTokenizer(location);
                              province = st.nextToken();
                              if (st.hasMoreElements())
                              {
                                  city = st.nextToken();
                              } else {
                                  city = null;
                              }
                          }
                          if(province!=null &&city != null &&text != null)
                          {
                              for(String keyword:keywordList)
                              {
                                  if(text.contains(keyword))
                                  {
                                      x.set(province+"\t"+city+"\t"+keyword);
                                      y.set(1+"");
                                      output.collect(x, y);
                                  }
                              }
                          }
                      }
                      
                  }
              }
          } catch (WeiboException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
          } catch (weibo4j.model.WeiboException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
            
          }
      }

      public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
          ArrayList<String> timeList = new ArrayList<String>();
          Long count = 0l;
          public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
          {
              count = 0l;
              while (values.hasNext()) 
              {
                  //values.next();
                  count+= Long.parseLong(values.next().toString());
              }
              output.collect(key, new Text(count+""));
          }
     }
      public static void main(String[] args) throws Exception {
          JobConf conf = new JobConf(PopularMotion.class);
          conf.setJobName("Popular motion per province");
          DistributedCache.addCacheFile(new URI("/home/haixinma/weibo/keyword.txt#keyword.txt"),conf);

          conf.setOutputKeyClass(Text.class);
          conf.setOutputValueClass(Text.class);

          conf.setMapperClass(Map.class);
          conf.setReducerClass(Reduce.class);
          conf.setInputFormat(TextInputFormat.class);
          conf.setOutputFormat(TextOutputFormat.class);
          conf.setNumReduceTasks(100);

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
