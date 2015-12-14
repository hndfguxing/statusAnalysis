package aprofile;

import java.io.*;
import java.util.regex.*;

public class DgetScreenNameByProfile {

	public static void main(String[] args) {
		String inPath = "./data/user.txt";
		String outPath = "";
		File inFile = new File(inPath);

		try {
			FileInputStream fis = new FileInputStream(inFile);
			InputStreamReader isr = new InputStreamReader(fis);
			BufferedReader br = new BufferedReader(isr);
			outPath = "./data/userScreenName.txt";
			File outFile = new File(outPath);
			FileOutputStream fos = new FileOutputStream(outFile);
			OutputStreamWriter osw = new OutputStreamWriter(fos,"utf-8");
			BufferedWriter bw = new BufferedWriter(osw);
			String line = "";
			while((line = br.readLine())!=null){
				Pattern pattern = Pattern.compile("(?<=\\bscreenName\\b:).*");
				Matcher matcher = pattern.matcher(line);
				if(matcher.find()){
					bw.write(matcher.group());
					bw.newLine();
				}
			}
			bw.flush();
			bw.close();
			osw.close();
			fos.close();
			br.close();
			isr.close();
			fis.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
