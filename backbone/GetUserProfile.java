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
import java.util.*;

/**
 * 描述：查询特定UID的用户信息 
 * 
 * 输入：微博集合、UID列表 
 * 输出：微作者UID\t微作者name 微作者粉丝数|#|微作者name|#|微作者微博数|#|微作者关注数|#|微作者收藏数|#|微作者性别|#|微作者地理位置|#|微作者认证|#|微作者创建日期
 */
public class GetUserProfile {
	
	
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
            		failedUnameSet.add(line);
            	}
            } catch (IOException e) {
            	e.printStackTrace();
            }
        }
        
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        	//输出:转微作者UID\t转微作者name  转微作者粉丝数|#|转微作者name|#|转微作者微博数|#|转微作者关注数|#|
        	//			转微作者收藏数|#|转微作者性别|#|转微作者认证|#|转微作者创建日期
        	//	   微作者UID\t微作者name  微作者粉丝数|#|微作者name|#|微作者微博数|#|微作者关注数|#|
        	//			微作者收藏数|#|微作者性别|#|微作者认证|#|微作者创建日期
        		
            try {
                retweetNum=0;
            	original=0;
            	maxFollower=0;
            	location=null;
            	record=null;
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
            
                if(statusList == null || statusList.size() ==0) {
                    return;
                }
               
                for(Status status : statusList) {
                    if(status.getUser() == null || status.getUser().getCreatedAt()==null)  {
                        continue;
                    }
                	if(status.getUser().getLocation()!=null){
                		location=status.getUser().getLocation();
                		
                    }
                	else{
                		location="";
                	}
                    
                    if(status.getUser().getFollowersCount()>maxFollower && failedUnameSet.contains(status.getUser().getName())){
                    	maxFollower=status.getUser().getFollowersCount();
                    	record=maxFollower+separator+status.getUser().getName()
                    	        +separator+status.getUser().getStatusesCount()+separator+status.getUser().getFriendsCount()
                    	        +separator+status.getUser().getFavouritesCount()+separator+status.getUser().getGender()
                    	        +separator+location+separator+status.getUser().isVerified()+separator+inputFormat.format(status.getUser().getCreatedAt());
                    }
                    
                    
                    if(status.getRetweetedStatus()!=null ) {
                        rtStatus = status.getRetweetedStatus();
                        if(rtStatus.getUser() == null || rtStatus.getUser().getCreatedAt()==null) {
                            continue;
                        }
                        if(!failedUnameSet.contains(rtStatus.getUser().getName())) {
                            continue;
                        }
                        if(rtStatus.getUser().getLocation()!=null) {
                            rtLocation=rtStatus.getUser().getLocation();
                            
                        }
                        else{
                            rtLocation="";
                        }
                        rtRecord=rtStatus.getUser().getFollowersCount()+separator+rtStatus.getUser().getName()
                                +separator+rtStatus.getUser().getStatusesCount()+separator+rtStatus.getUser().getFriendsCount()
                                +separator+rtStatus.getUser().getFavouritesCount()+separator+rtStatus.getUser().getGender()
                                +separator+rtLocation+separator+rtStatus.getUser().isVerified()+separator+inputFormat.format(rtStatus.getUser().getCreatedAt());
                    
                        if(rtStatus.getUser()!=null)
                        {
                            x.set(rtStatus.getUser().getId()+"\t"+rtStatus.getUser().getName());
                            y.set(rtRecord);
                            output.collect(x, y);
                        }
                    }
                    
                }
                
                if(maxFollower>0 && failedUnameSet.contains(statusList.get(0).getUser().getName())) {
                    x.set(statusList.get(0).getUser().getId()+"\t"+statusList.get(0).getUser().getName());
                    y.set(record);
                    output.collect(x, y); 
                }
                
            }
        
      }

      public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>  {
	      	//输入:转微作者UID\t转微作者name  转微作者粉丝数|#|转微作者name|#|转微作者微博数|#|转微作者关注数|#|
	      	//			转微作者收藏数|#|转微作者性别|#|转微作者地理位置|#|转微作者认证|#|转微作者创建日期
	      	//	   微作者UID\t微作者name  微作者粉丝数|#|微作者name|#|微作者微博数|#|微作者关注数|#|
	      	//			微作者收藏数|#|微作者性别|#|微作者地理位置|#|微作者认证|#|微作者创建日期
	    	//输出:微作者UID\t微作者name  微作者粉丝数|#|微作者name|#|微作者微博数|#|微作者关注数|#|
	      	//			微作者收藏数|#|微作者性别|#|微作者地理位置|#|微作者认证|#|微作者创建日期
    	  
          private final String separator = "|#|";
          
          public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        	  
        	  int max=0;
        	  String str=null;
      	  
        	  //获取该用户最大的粉丝数
              while (values.hasNext())  {
            	  String s=values.next().toString();
            	  String[] info=s.split("\\|#\\|");
            	  if(info.length>=1) {
                      int followers=Integer.valueOf(info[0]);
                      if(followers>max) {
                          str=s;  
                          max=followers;
                      }
            	  }
              } 
              
              if(str!=null) {
                  String[] strinfo=str.split("\\|#\\|");
                  if(str.length()>=9) {
                      str=max+separator+strinfo[1]+separator
                              +strinfo[2]+separator+strinfo[3]+separator+strinfo[4]+separator+strinfo[5]+separator
                              +strinfo[6]+separator+strinfo[7]+separator+strinfo[8];
                      output.collect(key, new Text(str));
                  }
              }
              
          }
          
      } 
      
      public static void main(String[] args) throws Exception {
          JobConf conf = new JobConf(GetUserProfile.class);
          conf.setJobName("Extract Failed User Info");
          DistributedCache.addCacheFile(new URI("/home/haixinma/weibo/failedUname#failedUname"),conf);
          conf.setOutputKeyClass(Text.class);
          conf.setOutputValueClass(Text.class);
          conf.setMapperClass(Map.class);
          conf.setReducerClass(Reduce.class);
          conf.setInputFormat(TextInputFormat.class);
          conf.setOutputFormat(TextOutputFormat.class);
          conf.setInt("mapred.min.split.size", 268435456);
          conf.setInt("mapred.task.timeout", 2400000);
          conf.setNumReduceTasks(1);
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

