package aprofile;

import java.io.*;

public class FcombineScreenNameAndUID {
	
	public static void main( String[] args ) {
		
		String lineName;
			
		try {
			BufferedReader brName = new BufferedReader(new InputStreamReader(new FileInputStream(new File("./data/userScreenName.txt"))));
			BufferedReader brID = new BufferedReader(new InputStreamReader(new FileInputStream(new File("./data/userID.txt"))));
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("C:/Users/Ray/Desktop/result/uid.txt"))));
			while((lineName = brName.readLine())!=null){ 
				bw.write(lineName);
				bw.write(":");
				bw.write(brName.readLine());
				bw.newLine();
				}
			bw.flush();
			bw.close();
			brID.close();
			brName.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
