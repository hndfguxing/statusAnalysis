package aprofile;

import java.io.*;

import weibo4j.Users;
import weibo4j.model.User;
import weibo4j.model.WeiboException;


public class BshowUserByDomainOrUID
{
	public static void main(String[] args)
	{
		String result = "";
		File file = new File("./data/aprofile/AuserDomainOrUID");
		File oFile = new File("./data/aprofile/Buser");
		try 
		{
			FileInputStream fis = new FileInputStream(file);
			InputStreamReader isr = new InputStreamReader(fis,"utf-8");
			BufferedReader br = new BufferedReader(isr);
			FileOutputStream fos = new FileOutputStream(oFile);
			OutputStreamWriter osw = new OutputStreamWriter(fos,"utf-8");
			BufferedWriter bw = new BufferedWriter(osw);
			String line = "";
			while((line = br.readLine()) != null)
			{
				if(line.length()!=10)
					result = ShowUserByDomain.run("", line);
				else
					result = ShowUserByUID.run("", line)	;	
				try 
				{
					long sleepTime=(long)(1+Math.random()*4)*1000;
					System.out.println("sleep"+sleepTime/1000+"s");
					Thread.sleep(sleepTime);
				} 
				catch (InterruptedException e)
				{
					e.printStackTrace();
				}
				bw.write(result);
				bw.newLine();
			}
			br.close();
			isr.close();
			fis.close();
			bw.flush();
			bw.close();
			osw.close();
			fos.close();
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		}	
	}
}

class ShowUserByDomain {

	public static String run(String access_token, String domain) {
		User user=null;
		Users um = new Users(access_token);
		try {
			user = um.showUserByDomain(domain);
		} catch (WeiboException e) {
			e.printStackTrace();
		}
		return user.toString();
	}

}

class ShowUserByUID {

	public static String run(String access_token, String uid) {
		Users um = new Users(access_token);
		User user=null;
		try {
			user = um.showUserById(uid);
		} catch (WeiboException e) {
			e.printStackTrace();
		}
		return (user.toString());
	}
}
