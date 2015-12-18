package Mood;

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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;

/**
 * 统计哪些天有用户注册
 * 
 * 输出:由用户注册的日期(年月日)
 */
public class UserRegisterTime {
    public static class Map extends MapReduceBase implements Mapper<LongWritable,Text, Text, Text> {
        private Text x = new Text();
        private Text y = new Text();
        ArrayList<String> tempList = new ArrayList<String>();
        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd 00:00:00");
        List<Status> statusList = new ArrayList<Status>();
        Status rtStatus = null;
        String endTime = null;
        String time = null;
        String text = null;
        String rtText = null;
        ArrayList<String> keywordList = new ArrayList<String>();
        private static String separator = "|#|";
        
        public void configure(JobConf job) 
        {
            Calendar c=Calendar.getInstance(); 
            c.set(2013,01,01,00,00,00);
            endTime = inputFormat.format(c.getTime());
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
                if(statusList == null)
                {
                    return;
                }
                for(Status status : statusList)
                {
                    if(status.getUser()!=null)
                    {
                        if(status.getUser().getCreatedAt()!=null)
                        {
                            x.set(status.getUser().getId());
                            y.set(inputFormat.format(status.getUser().getCreatedAt()));
                            output.collect(x, y);
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
          int count = 0;
          public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
          {
              count = 0;
              String time = "";
              while (values.hasNext()) 
              {
                  time = values.next().toString();
                  break;
              }
              output.collect(new Text(time), new Text(1+""));
          }
      }
      public static void main(String[] args) throws Exception {
          JobConf conf = new JobConf(UserRegisterTime.class);
          conf.setJobName("Register Time");

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
          conf.setNumReduceTasks(10);
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
