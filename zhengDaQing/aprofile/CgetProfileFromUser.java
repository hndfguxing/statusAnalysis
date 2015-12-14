package aprofile;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CgetProfileFromUser 
{
	public static void main(String[] args) 
	{
		File file = new File("./data/aprofile/Buser");
		try
		{
			FileInputStream fis = new FileInputStream(file);
			InputStreamReader isr = new InputStreamReader(fis,"utf-8");
			BufferedReader br = new BufferedReader(isr);
			String line = "";
			while((line = br.readLine()) != null)
			{
				
				Pattern pattern = Pattern.compile("(?<=\\bscreenName\\b=).*?(?=,)");
				Matcher matcher = pattern.matcher(line);  
				if(matcher.find())
				{  
					System.out.println("screenName"+":"+matcher.group());
				}
				
				pattern = Pattern.compile("(?<=\\bid\\b=).*?(?=,)");
				matcher = pattern.matcher(line);  
				if(matcher.find())
				{  
					System.out.println("id"+":"+matcher.group());
				}
				
				pattern = Pattern.compile("(?<=\\bfriendsCount\\b=).*?(?=,)");
				matcher = pattern.matcher(line);  
				if(matcher.find())
				{  
					System.out.println("friendsCount"+":"+matcher.group());
				}
				
				pattern = Pattern.compile("(?<=\\bfollowersCount\\b=).*?(?=,)");
				matcher = pattern.matcher(line);  
				if(matcher.find())
				{  
					System.out.println("followersCount"+":"+matcher.group());
				}
				pattern = Pattern.compile("(?<=\\bcreatedAt\\b=)(\\w*\\s?)*(:\\d\\d){2} (\\w*\\s?)*");
				matcher = pattern.matcher(line);  
				if(matcher.find())
				{  
					System.out.println("createdAt"+":"+matcher.group());
				}
				
				pattern = Pattern.compile("(?<=\\bstatusesCount\\b=).*?(?=,)");
				matcher = pattern.matcher(line);  
				if(matcher.find())
				{  
					System.out.println("statusesCount"+":"+matcher.group()+"\n\n");
				}	
				
				
			}
			br.close();
			isr.close();
			fis.close();
		} 
		catch (IOException e)
		{
			e.printStackTrace();
		}	
	}
}
