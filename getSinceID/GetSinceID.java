package updateStatus;

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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class GetSinceID {

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
        String profileImageUrl = null;
        String location = null;
        String description = null;
        ArrayList<String> keywordList = new ArrayList<String>();
        private static String separator = "|#|";
        boolean isRetweet = false;
        
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
                
                Status status=statusList.get(0);

                if(status.getUser()!=null&&status.getCreatedAt()!=null){
                	x.set(status.getUser().getId());
                	y.set(status.getCreatedAt()+"|#|"+status.getId()+"|#|"+status.getUser().getStatusesCount());
                    output.collect(x, y);
                }
                
            
            }
      }

      public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> 
      {
          public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
          {
        	  ArrayList<String> tempList=new ArrayList<String>();
        	  ArrayList<String> identifier=new ArrayList<String>();

              while (values.hasNext())
              {
            	  String value=values.next().toString();
            	  String info[]=value.split("\\|\\#\\|");
            	  if(!identifier.contains(info[1])){
            		  identifier.add(info[1]);
            		  tempList.add(value);
            	  }
              }

              //排序
              Collections.sort(tempList);
              String[] result=tempList.get(tempList.size()-1).split("\\|\\#\\|");

              //用户id，上次最后一条记录，微博数
              output.collect(new Text(""), new Text(key+"_"+result[1]+"_"+result[2]));
              
              tempList.clear();
              identifier.clear();
          }
      }

      
      public static void main(String[] args) throws Exception {
          JobConf conf = new JobConf(GetSinceId.class);
          conf.setJobName("ExtractSinceId");

          conf.setOutputKeyClass(Text.class);
          conf.setOutputValueClass(Text.class);
          conf.setMapperClass(Map.class);
          conf.setReducerClass(Reduce.class);
          conf.setInputFormat(TextInputFormat.class);
          conf.setOutputFormat(TextOutputFormat.class);

          conf.setInt("mapred.min.split.size", 268435456);
          conf.setInt("mapred.task.timeout", 2400000);
          conf.setNumReduceTasks(5);

          //第一参数是输入路劲
          FileInputFormat.setInputPaths(conf, new Path(args[0]));

          //第二个参数为输出路径
          Path output = new Path(args[1]);

          FileSystem fs = FileSystem.get(conf);
          if(fs.exists(output)){
            fs.delete(output,true);
          }
          FileOutputFormat.setOutputPath(conf, output);

          //运行
          JobClient.runJob(conf);
        }
}
