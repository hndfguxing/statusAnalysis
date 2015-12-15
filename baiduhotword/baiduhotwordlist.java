package baiduhotword;

import Util.IOTool;

import java.io.*;

/*
 * 事件、关键词组提取		
 */
public class baiduhotwordlist {
	public static void main(String[] args) throws IOException {
		
		
		File file=new File("E:\\sina\\百度热词");
		File[]files=file.listFiles();
		
		for(File f:files){
			
			FileInputStream fis0 = new FileInputStream(f);
			InputStreamReader isr0 = new InputStreamReader(fis0, "utf-8");
			BufferedReader br0 = new BufferedReader(isr0);
			
			String time=f.getName().substring(0,f.getName().indexOf(".txt"));
			String line;
		    String result = ""; 
			while((line = br0.readLine()) != null){
				line=line.replaceAll(" "+"\n", "\n");
				String[] info=line.split("\t");
					//System.out.println(info[3]);
				
				result+=info[1]+"\t";
			}
			IOTool.write("E:\\sina\\baiduhotwordlist_split.txt", time + "\t" + result, true, "utf-8");
		}
		
	}

}
