package drepost;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.*;

public class Bretweet {
	
	
//		0 rmid	1 mid	2 creatAtTime 	3 screenName 	4 text
	
		
		public static void main(String[] args) throws ParseException {
			
		
			Map<String, String> map = new HashMap<>();
			String line;
			try {
				BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("./data/aprofile/FscreenNameAndUID")),"utf-8"));
				while((line = br.readLine()) !=null){
					map.put(line.split(":")[1], line.split(":")[0]);
				}
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			File filePath = new File("./data/bstatus/crStatus");
			File[] files = filePath.listFiles();
			
			
		for(int i=0; i < files.length; i++){
				run(files[i].getAbsolutePath(), "./data/drepost/"+files[i].getName(),map);
				
			}
			
			
//			run("./data/bstatus/crStatus/3241456580", "./data/testResult1",map);
		}
		
		public static long dateConvert(String dateString) throws ParseException {
	
			SimpleDateFormat formatter = new SimpleDateFormat( "EEE MMM dd HH:mm:ss zzz yyyy", Locale.ENGLISH);
			Date date = formatter.parse(dateString);
			return(date.getTime());
		}
		
		
		public static void run(String input, String output, Map<String, String> srcName) throws ParseException {
			
//			传入一个输入文件，一个输出文件
//			输入文件是转发某个用户所有微博的微博
//			输出每条微博实际爬到的转发量，最早、最晚转发时间，第一次转发人数，最长转发量长度
			
			String line = null;
			Map<String, Map<String, String>> map = new HashMap<String, Map<String, String>>();
			Map<String, Map<String, String>> map1 = new HashMap<String, Map<String, String>>();
			File fileIn = new File(input);
			String rUID = fileIn.getName();
			String srcScreenName = srcName.get(rUID);
	
	
			
			try {

	    			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fileIn),"utf-8"));
					
					while((line = br.readLine()) != null){
						if(!(line.equals(""))){
					
							String[] status = {};
							status = line.split("\\\\\\|#\\|/");
							if(status.length >= 4){
								Map<String, String> mapC = null;
								Map<String, String> mapC1 = null;
								String rmid = status[0];
								String mid = status[1];
								String creatAtTime = (status[2]);
								String screenName = status[3];
								ArrayList<String> retweeter = new ArrayList<String>();
								
								if(status.length == 5){
									Pattern pattern = Pattern.compile("(?<=//@).+?(?=:)");
									Matcher matcher = pattern.matcher(status[4]);
									
								
									retweeter.add(screenName);
									while(matcher.find()) {
											retweeter.add(matcher.group());
									}
									retweeter.add(srcScreenName);
								}
								else {
									retweeter.add(screenName);
									retweeter.add(srcScreenName);
								}
	
								
								if(!map.containsKey(rmid)) {
	
									map.put(rmid, new HashMap<String, String>());
									map1.put(rmid, new HashMap<String, String>());
									mapC = map.get(rmid);
									mapC.put("maxTimeWL007", creatAtTime);
									mapC.put("minTimeWL007", creatAtTime);
									mapC.put("totalNumberWL007", "0");
								}
								

								mapC = map.get(rmid);
								mapC1 = map1.get(rmid);
								if(!mapC1.containsKey(mid)){
									mapC1.put(mid, "1");
									int totalNumberWl007 = Integer.parseInt(mapC.get("totalNumberWL007"))+1;
									mapC.put("totalNumberWL007", String.valueOf(totalNumberWl007));
								}
								
								if(dateConvert(creatAtTime) > dateConvert(mapC.get("maxTimeWL007")))
									mapC.replace("maxTimeWL007", creatAtTime);
								if(dateConvert(creatAtTime) < dateConvert(mapC.get("minTimeWL007")))
									mapC.replace("minTimeWL007", creatAtTime);
															
								for (int i = 0; i < retweeter.size()-1; i++) {	
										mapC.put(retweeter.get(i), retweeter.get(i+1));
								}
								
	
								}
							}
						}
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				ArrayList<String> tree = new ArrayList<String>();
				ArrayList<Integer> level = new ArrayList<Integer>();
				int tSize1 = 0;
				int tSize2 = 0;
				Map<String, String> mapC = new HashMap<String, String>();
				Map<String, String> keys = new HashMap<String, String>();
				File file = new File(output);
				if (file.exists())
					file.delete();
				
				for(String ruid: map.keySet()){
					
					tree.add(srcScreenName);
					level.add(new Integer(1));
					tSize1 = 0;
					tSize2 = tree.size();
					mapC = map.get(ruid);					
					for (String key : mapC.keySet()) {
						keys.put(key, "1");
					}
					while (keys.size()>3){
						for(int i = tSize1; i < tSize2; i++){
							for (String key : mapC.keySet()) {
								if(!key.equals("totalNumberWL007")&&
										!key.equals("minTimeWL007")&&
										!key.equals("maxTimeWL007")){
									
										String val = mapC.get(key);
										if (val.equals(tree.get(i))) {
											keys.remove(key);
											if (!tree.contains(key)) 
												tree.add(key);
										}
								}
			
							}
						}
						level.add(new Integer(tree.size()));
						tSize1 = tSize2;
						tSize2 = tree.size();
					}
						
					
					try {

						if (!file.exists()) 
							file.createNewFile();
						BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file,true), "utf-8"));
						bw.write(ruid+"\\|#|/");
						bw.write(mapC.get("totalNumberWL007")+"\\|#|/");
						bw.write(new Date(dateConvert(mapC.get("minTimeWL007"))).toString()+"\\|#|/");
						bw.write(new Date(dateConvert(mapC.get("maxTimeWL007"))).toString()+"\\|#|/");
						bw.write(level.get(1)-1+"\\|#|/");
						bw.write(level.size()-1+"\\|#|/");
						bw.newLine();
						bw.flush();
						bw.close();

					} catch (IOException e) {
						e.printStackTrace();
					}
					
					tree.clear();;
					level.clear();
					mapC.clear();
					keys.clear();
					
				}
				
				
				System.out.println("complete\t"+output);
	
			}
		
}
