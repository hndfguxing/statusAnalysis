package aprofile;

import java.io.*;

public class HcombineProfileAndVFollowerCount {
	
	public static void main(String[] args) {
		
		int i = 1;
		String line = null;
		
		try {
			BufferedReader br1 = new BufferedReader(new InputStreamReader(new FileInputStream(new File("./data/aprofile/Cprofile"))));
			BufferedReader br2 = new BufferedReader(new InputStreamReader(new FileInputStream(new File("./data/aprofile/GvFollowerCount"))));
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("./data/aprofile/HprofileNew")), "utf-8"));
			while(i<=42){
				line = br1.readLine();
				bw.write(line);
				bw.newLine();
				line = br1.readLine();
				bw.write(line);
				bw.newLine();
				line = br1.readLine();
				bw.write(line);
				bw.newLine();
				line = br2.readLine();
				bw.write("verifiedFollowersCount/getFolowersCount:");
				bw.write(line);	
				bw.newLine();
				line = br1.readLine();
				bw.write(line);
				bw.newLine();
				line = br1.readLine();
				bw.write(line);
				bw.newLine();
				line = br1.readLine();
				bw.write(line);
				bw.newLine();
				line = br1.readLine();
				line = br1.readLine();
				bw.write(line);
				bw.newLine();
				bw.flush();
				i++;
			}
			bw.flush();
			bw.close();
			br1.close();
			br2.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}
