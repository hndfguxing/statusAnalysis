package baiduhotword;

import Util.IOTool;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/*
 * 合并hotwordcount类输出数据中的重复项
 */
public class mergereuslt {
	public static void main(String[] args) throws IOException {
		
		Map<String, String> average = new HashMap<String, String>();
		
		File f=new File("Y:\\code\\data\\zhongkeyuan\\zhongfinal_twlkrt");
        File[]files=f.listFiles();
        
        for(File file:files){
			FileInputStream fis0 = new FileInputStream(file);
			InputStreamReader isr0 = new InputStreamReader(fis0, "utf-8");
			BufferedReader br0 = new BufferedReader(isr0);
	        System.out.println(file.getName()+"hha");
			String line;
	
			while ((line = br0.readLine()) != null) {
				String[] info = line.split("\t");
				//Tool.write("E:\\sina\\zhongkeyuan\\rt_p.txt", line,true, "utf-8");
				//System.out.println(line);
				if(info[2].contains(" ")){
					String p=info[2].substring(0,info[2].indexOf(" "));		//info[2] location
					if(p.equals("北京")||p.equals("上海")||p.equals("重庆")||p.equals("天津")||p.equals("香港")||p.equals("澳门")){
					info[2]=p;
					}
				}
				if (!average.containsKey(info[0]+"\t"+info[1]+"\t"+info[2])) {
				
					average.put(info[0]+"\t"+info[1]+"\t"+info[2], info[3]);
				}else{
					String count=average.get(info[0]+"\t"+info[1]+"\t"+info[2]);
					int newCount=Integer.valueOf(count)+Integer.valueOf(info[3]);
					
					average.put(info[0]+"\t"+info[1]+"\t"+info[2], String.valueOf(newCount));
				}
	
			}

        }
        
        
		Iterator<Entry<String, String>> it2 = average.entrySet().iterator();
		
		while (it2.hasNext()) {
			Entry<String, String> entry = it2.next();
			
			IOTool.write("E:\\sina\\zhongkeyuan\\twlkrt_c.txt", entry.getKey() + "\t" + entry.getValue(), true, "utf-8");

		}

	}

	}
