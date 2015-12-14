package aprofile;

import java.io.*;

import weibo4j.Friendships;
import weibo4j.model.User;
import weibo4j.model.UserWapper;
import weibo4j.model.WeiboException;
import yutil.Sleep;

public class GgetVFollowerCountByUID {

	public static void main(String[] args) {
		
		String line = null;
		int[] count = {0,0,0};
		
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("./data/aprofile/EuID"))));
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("./data/aprofile/GfollowerCount")), "utf-8"));
			while((line = br.readLine()) !=null){
				count = getCount(line);
				bw.write(String.valueOf(count[1]));
				bw.write("/"+String.valueOf(count[0]));
				bw.newLine();
				bw.flush();
			}
			bw.flush();
			bw.close();
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
	
	public static int[] getCount(String uid){
		
		String access_token = "";
		Friendships fm = new Friendships(access_token);
		int countV = 0;
		int countG = 0;
		int[] ret = {0,0,0};
		long cursor = 0;
		long totalNumber = 0;
		
		try {
			UserWapper users = fm.getFollowersById(uid, 200, (int)cursor);
			totalNumber = users.getTotalNumber();
			cursor = users.getNextCursor();
			for(User u : users.getUsers()){
				countG++;
				if (u.isVerified()) 
					countV++;
			}	
		} catch (WeiboException e) {
			e.printStackTrace();
		}
		
		Sleep.run(1,5);
		
		while (cursor != 0){
			
			try {
				UserWapper users = fm.getFollowersById(uid, 200, (int)cursor);
				cursor = users.getNextCursor();
				for(User u : users.getUsers()){
					countG++;
					if (u.isVerified()) 
						countV++;
				}	
			} catch (WeiboException e) {
				e.printStackTrace();
			}
			
			Sleep.run(1,5);
			
		}
		
		ret[0] = countG;
		ret[1] = countV;
		ret[2] = (int)totalNumber;
		return ret;
		
	}

}
