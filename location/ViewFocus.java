package Location;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

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

/**
 * 计算每条转发微博的转发者的location的熵 即纯度的度量
 * 
 * 输出:转发微博MID  转发微博的转发量  熵
 */
public class ViewFocus{public static class Map extends MapReduceBase implements Mapper<LongWritable,Text, Text, Text> {
    private Text x = new Text();
    private Text y = new Text();
    ArrayList<String> tempList = new ArrayList<String>();
    SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    List<Status> statusList = new ArrayList<Status>();
    Status rtStatus = null;
    String endTime = null;
    String time = null;
    String location = null;
    String province = null;
    String city = null;
    private static String separator = "|#|";
    
    public void configure(JobConf job) 
    {
        Calendar c=Calendar.getInstance(); 
        c.set(2012,01,01,00,00,00);
        endTime = inputFormat.format(c.getTime());
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
              if(time.compareTo(endTime)<0 &&status.getRetweetedStatus() !=null)
              {
                  rtStatus = status.getRetweetedStatus();
                  if(rtStatus!=null && status.getUser()!=null)
                  {
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
                      if(province!=null && city!= null)
                      {
                          x.set(rtStatus.getMid());
                          y.set(province+"\t"+city);
                          output.collect(x, y);
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

  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> 
  {
      ArrayList<String> timeList = new ArrayList<String>();
      Long count = 0l;
      String province = null;
      int max=0;
      double entropy = 0;
      java.util.Map<String,Integer> provinceCountMap = new HashMap<String,Integer>();
      public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
      {
          max  = 1;
          count = 0l;
          entropy = 0;
          while (values.hasNext()) 
          {
              ++count;
              province = values.next().toString();
              if(provinceCountMap.containsKey(province))
              {
                  int temp = provinceCountMap.get(province);
                  if(max<temp+1)
                  {
                      max = temp+1;
                  }
                  provinceCountMap.put(province, ++temp);
              }else
              {
                  provinceCountMap.put(province,new Integer(1));
              }
          }
          //compute the entropy
          Iterator iterator = provinceCountMap.entrySet().iterator();
          while (iterator.hasNext())
          {
              java.util.Map.Entry<String,Integer> entry = (java.util.Map.Entry<String,Integer>) iterator.next();
              Integer value = entry.getValue();
              entropy += (-1)*(((double)value)/count)*(Math.log(((double)value)/count)/Math.log(2));
          }
          provinceCountMap.clear();
          output.collect(key, new Text(count+"\t"+entropy));
      }
  }
  public static void main(String[] args) throws Exception {
      JobConf conf = new JobConf(ViewFocus.class);
      conf.setJobName("View Entropy city");

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
