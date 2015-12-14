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

public class ExtractMentionship {
	
    public static class Map extends MapReduceBase implements Mapper<LongWritable,Text, Text, Text> {
    	
        private Text x = new Text();
        private Text y = new Text();
        ArrayList<String> tempList = new ArrayList<String>();
        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        List<Status> statusList = new ArrayList<Status>();
        Status rtStatus = null;
        String endTime = null;
        String startTime = null;
        String time = null;
        String text = null;
        String rtText = null;
        String rtMid = "";
        ArrayList<String> keywordList = new ArrayList<String>();
        
        public void configure(JobConf job) {
        	Calendar c=Calendar.getInstance(); 
            c.set(2012,02,01,00,00,00);
            Calendar s=Calendar.getInstance(); 
            s.set(2011,12,01,00,00,00);
            endTime = inputFormat.format(c.getTime());
            startTime = inputFormat.format(s.getTime());
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
	            	keywordList.add(line);		//转发微博作者的name列表
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
            	if(status.getRetweetedStatus()!=null){
            		if(status.getRetweetedStatus().getUser()==null){
            			continue;
            		}
            		if(keywordList.contains(status.getRetweetedStatus().getUser().getName())){
            			String retext=status.getRetweetedStatus().getText();
            			List<String> reresult=MentionList(retext);
            			if(reresult!=null){
             		    for(int i=0;i<reresult.size();i++){
             		    if(keywordList.contains(reresult.get(i))){
             		    	x.set(status.getRetweetedStatus().getUser().getName()+"\t"+reresult.get(i)+"\t"+inputFormat.format(status.getRetweetedStatus().getCreatedAt()));
             		    	y.set(new Text("1"));
             		    	output.collect(x,y);
             		    }
             		    }
            			}
            		}
            	}
            	
            	if(status.getUser()==null){
        			continue;
        		}
            	if(keywordList.contains(status.getUser().getName())){
            		String text=status.getText();
            		List<String> result=MentionList(text);
            		if(result!=null){
	            		for(int i=0;i<result.size();i++){
		            		if(keywordList.contains(result.get(i))){
		            			x.set(status.getUser().getName()+"\t"+result.get(i)+"\t"+inputFormat.format(status.getCreatedAt()));
		            		    y.set(new Text("1"));
		            		    output.collect(x,y);
		            		}
	            		}
            		}
            	}
            }
            
        }
        
        public List<String> MentionList(String text){
        	//找出微博内容中@的所有人,不包括//@
        	
        	List<String> tempList=new ArrayList<String>();
        	final String regexMention = "@[^@\\s]+";
        	if(text.contains("//@")){
        		text=text.substring(0,text.indexOf("//@"));
        		
        	}
        	
        	Pattern patternMention = Pattern.compile(regexMention);
    		Matcher m = patternMention.matcher(text);
    		while(m.find()){
    			if(tempList.contains(m.group()))
    				continue;
    			tempList.add(m.group());
    		}
    		return tempList;
        }
       
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		
    	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        	while(values.hasNext()){  
        		output.collect(key, new Text(""));
        		break;
        	}
         }
    }
      
	public static void main(String[] args) throws Exception {
    	JobConf conf = new JobConf(ExtractMentionship.class);
		conf.setJobName("extract mentionList");
		DistributedCache.addCacheFile(new URI("/home/zhangqunyan/input/uname4mention#uname4mention"),conf);
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

