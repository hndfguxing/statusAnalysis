package backbone;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
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

import weibo4j.model.Status;

/**
 * 描述：根据//@A://@B://@C:还原边A B和B C 
 * 
 * 输入：微博、微博作者、转发微博作者 
 * 输出：边A B
 */
public class BackboneEdge {
	
    public static class Map extends MapReduceBase implements Mapper<LongWritable,Text, Text, Text> {
    	
        private Text x = new Text();
        private Text y = new Text();
        ArrayList<String> tempList = new ArrayList<String>();
        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        List<Status> statusList = new ArrayList<Status>();
        Status rtStatus = null;
        String time = null;
        String text = null;
        String rtText = null;
        String rtMid = "";
        ArrayList<String> keywordList = new ArrayList<String>();
        
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        	
            String record = value.toString();
            if(record.contains("\t") && !record.contains("end of retweet")) {
                String rtMid = record.substring(0, record.indexOf("\t"));
                if(!rtMid.equals("")) {
                    x.set(rtMid);
                    y.set(record.substring(record.indexOf("\t")+1));
                    output.collect(x, y);
                }
            }
            
        }
           
	}

      public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    	  
          ArrayList<String> tempList = new ArrayList<String>();
          ArrayList<String> identifier = new ArrayList<String>();
          ArrayList<String> rtEdgeList = new ArrayList<String>();
          
          public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        	  //输入:转发微博MID  转发微博的其他信息
        	  
              String uname = null;
              String uname1 = null;
              while (values.hasNext()) {
                  String s=values.next().toString();
                  String[] info=s.split("\\|#\\|");
                  if(!identifier.contains(info[1])){
                      identifier.add(info[1]);
                      tempList.add(s);
                  }
              }
                  Collections.sort(tempList);
                  
                  //根据//@A://@B://@C:还原边A B和B C
                  for(String temp:tempList) {
                      String[] list = temp.split("\\|#\\|");
                      if(list.length<10) {
                          continue;
                      }
                      if(list[2].contains(":")) {
                          list[2] = list[2].replaceAll(":", "：");
                      }
                      if(list[2].contains("// @")) {
                          list[2] = list[2].replaceAll("// @", "//@");
                      }
                      if(list[2].contains(" ")) {
                          list[2] = list[2].replaceAll(" ", "");
                      }
                      String[] contentList = list[2].split("//@");
                      if(contentList.length == 1) {
                          rtEdgeList.add(list[5]+"\t"+list[10]);		//list[10]是转发微博的作者  list[5]是转发微博的作者 
                      }
                      else if(contentList.length > 1) {
                          uname = contentList[contentList.length-1];		
                          if(uname.contains("："))
                          {
                              uname = uname.substring(0,uname.indexOf("："));
                              if(!rtEdgeList.contains(uname+"\t"+list[10]))
                              {
                                  rtEdgeList.add(uname+"\t"+list[10]);
                              }
                          }
                          for(int j = contentList.length-2;j>0;j--) {
                              uname = contentList[j];
                              uname1 = contentList[j+1];
                              if(uname.contains("：") && uname1.contains("：")) {
                                  uname = uname.substring(0,uname.indexOf("："));
                                  uname1 = uname1.substring(0,uname1.indexOf("："));
                                  if(!rtEdgeList.contains(uname+"\t"+uname1)) {
                                      rtEdgeList.add(uname+"\t"+uname1);
                                  }
                              }
                          }
                          uname = contentList[1];
                          if(uname.contains("："))
                          {
                              uname = uname.substring(0,uname.indexOf("："));
                              rtEdgeList.add(list[5]+"\t"+uname);
                              if(!rtEdgeList.contains(list[5]+"\t"+uname))
                              {
                                  rtEdgeList.add(list[5]+"\t"+uname);
                              }
                          }
                      }                 
                  }
                  
                  for(String rtEdge:rtEdgeList)
                  {
                      output.collect(key,new Text(rtEdge));
                  }
                  rtEdgeList.clear();
                  output.collect(new Text(""),new Text("end of retweet"));

              tempList.clear();
              identifier.clear();
              
          }
          
      }
      
      public static void main(String[] args) throws Exception {
          JobConf conf = new JobConf(BackboneEdge.class);
          conf.setJobName("BackboneEdge");
          conf.setOutputKeyClass(Text.class);
          conf.setOutputValueClass(Text.class);
          conf.setMapperClass(Map.class);
          conf.setReducerClass(Reduce.class);
          conf.setInputFormat(TextInputFormat.class);
          conf.setOutputFormat(TextOutputFormat.class);
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
