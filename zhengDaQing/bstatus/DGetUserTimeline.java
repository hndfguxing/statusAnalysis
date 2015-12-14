package bstatus;

import java.io.*;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import weibo4j.Timeline;
import weibo4j.model.Paging;
import weibo4j.model.Status;
import weibo4j.model.StatusWapper;
import weibo4j.model.WeiboException;

public class DGetUserTimeline {

	public static void main(String[] args) {
		
		String access_token=null;
		Timeline tm = null;
		String uid = "";
		String line ="";
		long totalNumber = 100;
		int count = 2;
		int counts = 0;
		String screenName = null;
		int i = 0;
		ArrayList<String> tokens = new ArrayList<String>();
		
		//将token添加到tokens中
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("./data/tokens"))));
			for (int u = 0; u < 50; u++) 
				tokens.add(br.readLine());
			br.close();
			} catch (Exception e) {
			}
		
		
		
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("./data/aprofile/Cprofile"))));
			while ((line=br.readLine())!=null){
				count = 2;
				counts = 0;
				i++;
				access_token=tokens.get(i);
				tm = new Timeline(access_token);
				
				//读入用户screenName作为输入用户名，读入UID
				Pattern pattern = Pattern.compile("(?<=\\bscreenName\\b:).*");
				Matcher matcher = pattern.matcher(line);
				if(matcher.find()){
					screenName=(matcher.group());
				}
				line=br.readLine();
				pattern = Pattern.compile("(?<=\\bid\\b:).*");
				matcher = pattern.matcher(line);
				if(matcher.find()){
					uid=(matcher.group());
				}
				line=br.readLine();
				line=br.readLine();
				line=br.readLine();
				line=br.readLine();
				line=br.readLine();
				line=br.readLine();
	
				//将爬取得微博放入./data/screenName中，如果已存在，则删除
				File ofile = new File("./data/"+screenName);
				if (ofile.exists()) ofile.delete();
				FileOutputStream ops = new FileOutputStream(ofile,true);
				OutputStreamWriter osw = new OutputStreamWriter(ops,"utf-8");
				BufferedWriter bw = new BufferedWriter(osw);
				
				//爬取微博
				for (int pageth = 1; count > 1; pageth++) {
					Paging page=new Paging(pageth,100);
					
					try {
						StatusWapper statusWapper = tm.getUserTimelineByUid(uid,page,0,0);
						int j = statusWapper.getStatuses().size();
						count = j;
						counts += j;
					
						//将每一条微博写入文件
						for(Status status:statusWapper.getStatuses()){	
							String picture = null;
							if (!status.getOriginalPic()
									.equals(""))
								picture = "1\\|#|/"
										+ status.getOriginalPic();
							else
								picture = "0";
							line = status.getMid()+"\\|#|/"+
									status.getText()+"\\|#|/"+
									status.getCommentsCount()+"\\|#|/"+
									status.getRepostsCount()+"\\|#|/"+
									status.getAttitudesCount()+"\\|#|/"+
									picture;
							totalNumber = statusWapper.getTotalNumber();
							bw.write(line);
							bw.newLine();						
						}
						
					} catch (WeiboException e) {
						e.printStackTrace();
					}
			
					//休眠1-5秒
					int ss=(int)(1+Math.random()*5)*1000;
					System.out.println("get"+count+"----"+"sleep"+ss/1000);
					try {
						Thread.sleep(ss);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}//for end
				
			//写入本次写入数量以及微博的总数
			bw.write("counts="+counts+"\t");
			bw.write("totalNumber="+String.valueOf(totalNumber));
			bw.newLine();
			System.out.println(counts);
			bw.flush();
			bw.close();
			osw.close();
			ops.close();
			ofile.renameTo(new File("./data/"+screenName+counts));
			
			}
		br.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
	}
	
}
