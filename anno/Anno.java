import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import weibo4j.model.Status;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Anno {
	//匿名
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
        String rtMid = null;
        String profileImageUrl = null;
        String location = null;
        String description = null;
        ArrayList<String> keywordList = new ArrayList<String>();
		ArrayList<String> moodList = new ArrayList<String>();
        boolean isRetweet = false;
     
        public void configure(JobConf job) 
        {
			FileInputStream fis;
			InputStreamReader isr;
			BufferedReader br;
			
			FileInputStream fis1;
			InputStreamReader isr1;
			BufferedReader br1;
			String line = null;
			try {
				Path[] file = DistributedCache.getLocalCacheFiles(job);
				fis = new FileInputStream(file[0].toString());
				isr = new InputStreamReader(fis, "utf8");
				br = new BufferedReader(isr);
				while ((line = br.readLine()) != null) {
					keywordList.add(line);
				}
				
				fis1 = new FileInputStream(file[1].toString());
				isr1 = new InputStreamReader(fis1, "utf8");
				br1 = new BufferedReader(isr1);
				while ((line = br1.readLine()) != null) {
					moodList.add(line.split("\t")[0]);
				}
			
			} catch (IOException e) {
				e.printStackTrace();
			}
        }
        
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            try {
            	
            		String line = value.toString();
            		String remid = line.split("\\|\\#\\|")[1].split("\t")[0];
        			String hexRemid = "";
        			String orimid = line.split("\\|\\#\\|")[2];
        			String hexOrimid = "";
        			String userid = line.split("\\|\\#\\|")[8];
        			String hexUserid = "";
        			String username = line.split("\\|\\#\\|")[9];
        			String hexUsername = "";
        			String text = line.split("\\|\\#\\|")[4];
        			String location = "";
        			
        			if(line.split("\\|\\#\\|").length>9){
        				for (int i = 0; i < remid.length(); i++) {
							hexRemid += Integer.toHexString(remid.charAt(i));
						}
										
						if(orimid.equals(" "))
							hexOrimid = " ";
						else{
							for (int i = 0; i < orimid.length(); i++) {
								hexOrimid += Integer.toHexString(orimid.charAt(i));
							}
						}
						
						for (int i = 0; i < userid.length(); i++) {
							hexUserid += Integer.toHexString(userid.charAt(i));
						}
						
						for (int i = 0; i < username.length(); i++) {
							hexUsername += Integer.toHexString(username.charAt(i));
						}
						
						if(line.split("\\|\\#\\|").length==11)
							location = line.split("\\|\\#\\|")[10];
						else
							location = " ";
						
						String regEx = "(@[^\\s:：，,.。@]*(?=[\\s:：，,.。]))|(@[\\wu4e00-u9fa5_-]+[^\\s])";
						Pattern pattern = Pattern.compile(regEx);
						Matcher matcher = pattern.matcher(text);
						String atText = "";
						while(matcher.find()){
							String miduser = matcher.group(0);
							miduser = miduser.substring(miduser.indexOf("@")+1);
							String hexMid = "";
							for (int i = 0; i < miduser.length(); i++) {
								hexMid += Integer.toHexString(miduser.charAt(i));
							}
							atText += "@"+hexMid+"\t";
						}
						
						String keyText = "";
						String regEx1="[。？！?.!]";
				        Pattern p =Pattern.compile(regEx1);
				        Matcher m = p.matcher(text);
				        String[] substrs = p.split(text);
	                    for(String keyword:keywordList)
	                    {
	                    	if(keyword.split("\t")[0].equals(line.split("\\|\\#\\|")[0])){
	                    		for(String s:substrs){
	                    		for(int i=0;i<keyword.split("\t")[1].split("\\|").length;i++){
	                    			
	                    				if(s.contains(keyword.split("\t")[1].split("\\|")[i]))
	                    				keyText +=s;
	                    				break;
	                    			}
	                    		}
	                    	}
	                    }
						
						String moodText = "";
						for(String moodword:moodList){
							if(text.contains(moodword))
								moodText += moodword+"\t";
						}
						
	                    String finalText = "";
	                    finalText = atText + keyText + "|#|" + moodText;
	                    
	                    x.set(line.split("\\|\\#\\|")[0]+"|#|"+
	                    		hexRemid+"\t"+line.split("\\|\\#\\|")[1].split("\t")[1]+"|#|"+
	                    		hexOrimid+"|#|"+
	                    		line.split("\\|\\#\\|")[3]+"|#|"+
	                    		finalText+"|#|"+
	                    		line.split("\\|\\#\\|")[5]+"|#|"+
	                    		line.split("\\|\\#\\|")[6]+"|#|"+
	                    		line.split("\\|\\#\\|")[7]+"|#|"+
	                    		hexUserid+"|#|"+
	                    		hexUsername+"|#|"+
	                    		location);
	                    output.collect(x, y);
        			}
                    

            }catch(Exception e){
            	e.printStackTrace();
            }
      }
    }
      
      public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> 
      {
          public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
          {
              while (values.hasNext()) 
              {
                  output.collect(key, new Text(values.next()));
              }
          }
      }
      
      
      public static void main(String[] args) throws Exception {
          JobConf conf = new JobConf(Anno.class);
          conf.setJobName("Anno");
          DistributedCache.addCacheFile(new URI("/home/haixinma/weibo/keywordList.txt#keywordList.txt.txt"),conf);
		  DistributedCache.addCacheFile(new URI("/home/haixinma/weibo/motion.txt#motion.txt.txt"),conf);
          conf.setOutputKeyClass(Text.class);
          conf.setOutputValueClass(Text.class);
          conf.setMapperClass(Map.class);
          conf.setReducerClass(Reduce.class);
          conf.setInputFormat(TextInputFormat.class);
          conf.setOutputFormat(TextOutputFormat.class);
          conf.setInt("mapred.min.split.size", 268435456);
          conf.setInt("mapred.task.timeout", 2400000);
          conf.setNumReduceTasks(10);
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
