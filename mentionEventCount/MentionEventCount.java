package MentionEventCount;

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
 * 1. 计算特定用户微博、转发微博中提到特定事件的次数
 * 2. 粉丝大于1000的用户
 * 3. 2013年1月1日前发布的微博
 * 4. 微博或转发微博中包含有特定事件的关键字，则总次数+1
 * 
 * 输入：微博集合、特定事件、特定事件的关键字 
 * 输出：微博作者UID 特定事件 提及特定事件的次数
 */
public class MentionEventCount {
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable,Text, Text, Text> {
    	
        private Text x = new Text();
        private Text y = new Text();
        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        List<Status> statusList = new ArrayList<Status>();
        String endTime = null;
        String time = null;
        String text = null;
        String rtText = null;
        ArrayList<String> keywordList = new ArrayList<String>();
        
        public void configure(JobConf job) {
        	
            Calendar c=Calendar.getInstance(); 
            c.set(2013,01,01,00,00,00);			//2013年1月1日为结束日期
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
            	while((line = br.readLine()) != null) {
            		keywordList.add(line);		//读取事件以及事件的关键字
            	}
            } catch (IOException e) {
              e.printStackTrace();
            }
            
        }
        
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        	
        	try {
        		
        		//解析微博
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
                
                if(statusList == null) {
                    return;
                }
                for(Status status : statusList) {
                    time = inputFormat.format(status.getCreatedAt());
                    if(time.compareTo(endTime)<0) {
                        if(status.getUser()!=null) {
                            if(status.getUser().getFollowersCount()>=1000) {				//选择粉丝大于1000的发布日期在2013年1月1日前的微博
                                text = status.getText();
                                if(status.getRetweetedStatus()!=null) {
                                    rtText = status.getRetweetedStatus().getText();
                                }else {
                                    rtText = "";
                                }
                                for(String keyword:keywordList) {
                                    String[] list = keyword.split("\t");					//list[1]为事件的关键字  list[0]为事件
                                    String find = list[1];                      
                                    Pattern p = Pattern.compile(find);
                                    Matcher matcher = p.matcher(text+rtText); 				//如果在微博中或者转发的微博中包含有事件的关键字
                                    if(matcher.find()) {
                                        x.set(status.getUser().getId()+"\t"+list[0]);		//输出:key-微博作者的UID  事件  value-1
                                        y.set(1+"");
                                        output.collect(x, y);
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (WeiboException e) {
                e.printStackTrace();
            } catch (weibo4j.model.WeiboException e) {
              e.printStackTrace();
            }
        	
        }//map method end
        
	}//Map class end

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>  {
		
			int count = 0;		//提及事件的次数
          
			public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        	  //输入:key-微博作者的UID  事件  value-1
        	  
				while (values.hasNext()) 
				{
					++count;
					values.next();
				}
				output.collect(key, new Text(count+""));		//输出:key-微博作者的UID  事件  value-该用户提及事件的次数
              
			}//reduce method end
          
	}//Reduce class end
	
	public static void main(String[] args) throws Exception {
		
			JobConf conf = new JobConf(MentionEventCount.class);
			conf.setJobName("User+Event+Count");
			DistributedCache.addCacheFile(new URI("/home/haixinma/weibo/political.txt#political.txt"),conf);
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
