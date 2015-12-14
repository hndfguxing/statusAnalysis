package bstatus;

import java.io.*;

public class CMIDListAndstatus {
	
	public static void main(String[] args) {
		
		String line = null;
		String fileName = null;
		BufferedWriter bwMIDList = null;
		BufferedWriter bwStatus = null;
		BufferedWriter bwRStatus = null;
		BufferedWriter bwFailStatus = null;
		
		try {
			
			File filePath = new File("./data/bstatus/apart/2014");
			File[] files = filePath.listFiles();
			for(int i=0; i < files.length; i++){
				
				BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(files[i])));
				
				while((line=br.readLine()) != null){

				if(line.charAt(0)!='r'){
						
						fileName = line.substring(0, 10);
						
						File bStatusFile = new File("./data/bStatus/bstatus/"+fileName);
						if (!bStatusFile.exists())
							bStatusFile.createNewFile();
						bwStatus  = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(bStatusFile,true), "utf-8"));
						bwStatus.write(line.substring(11));
						bwStatus.newLine();
						
						File bmIDListFile = new File("./data/bStatus/bmIDList/"+fileName);
						if (!bmIDListFile.exists())
							bmIDListFile.createNewFile();
						bwMIDList  = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(bmIDListFile,true), "utf-8"));
						bwMIDList.write(line.substring(11, 27));
						bwMIDList.newLine();
						bwStatus.flush();
						bwStatus.close();
						bwMIDList.flush();
						bwMIDList.close();
				}
				
				else if (line.charAt(0)=='r'){
					
					fileName = line.substring(1, 11);
					File brStatusFile = new File("./data/bStatus/brStatus/"+fileName);
					if (!brStatusFile.exists())
						brStatusFile.createNewFile();
					bwRStatus  = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(brStatusFile,true), "utf-8"));
					bwRStatus.write(line.substring(12));
					bwRStatus.newLine();
					bwRStatus.flush();
					bwRStatus.close();
					
				}
				
				else {
					
					File brStatusFile = new File("./data/bStatus/failStatus");
					if (!brStatusFile.exists())
						brStatusFile.createNewFile();
					bwFailStatus  = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(brStatusFile,true), "utf-8"));
					bwFailStatus.write(line);
					bwFailStatus.newLine();
					bwFailStatus.flush();
					bwFailStatus.close();
					
				}
						
				}
				
				br.close();
				
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}
