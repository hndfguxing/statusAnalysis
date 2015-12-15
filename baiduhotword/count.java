package baiduhotword;

import Util.IOTool;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

/*
 * 统计讨论特定事件的微博数
 */
public class count {

	public static void main(String[] args) throws IOException {
		
		Map<String,List<String>> hotwordlist = new HashMap<String,List<String>>();
		Map<String,Map> result = new HashMap<String,Map>();
		
		FileInputStream fis1;
        InputStreamReader isr1;
        BufferedReader br1;
        String line1 = null;
        
        //将事件 关键词组列表添加到hotwordlist中
		fis1 = new FileInputStream("E:\\sina\\baiduhotwordlist.txt");
    	isr1 = new InputStreamReader(fis1,"utf8");
        br1 = new BufferedReader(isr1);
        while((line1 = br1.readLine()) != null)
        {
        	String[] info=line1.split("\t");
        	if(!hotwordlist.containsKey(info[0])){
        		List<String> l=new ArrayList<String>();
        		for(int i=1;i<info.length;i++){
        			l.add(info[i]);
        		}
                hotwordlist.put(info[0],l);
        	}
        	
        }
        br1.close();
		
		File f=new File("Y:\\code\\data\\zhongkeyuan\\BaiduTimeStatus4");
        File[]files=f.listFiles();
        
        for(File file:files){
			FileInputStream fis0 = new FileInputStream(file);
			InputStreamReader isr0 = new InputStreamReader(fis0, "utf-8");
			BufferedReader br0 = new BufferedReader(isr0);
	        System.out.println(file.getName());
			String value;
	
			while ((value = br0.readLine()) != null) {
	
	            value=value.replaceAll("\t", "|#|");
	                	
	            String[] info=value.split("\\|"+"\\#"+"\\|");
	                	
	            if(info.length==4){
	            	String text=info[2];									//微博内容
	                if(hotwordlist.containsKey(info[1])){					//事件
	                	List<String> wordlist=hotwordlist.get(info[1]);		//关键词组列表
	                	
	                	for(String word:wordlist){							//关键词组
	                		String[]subword=word.split(" ");				
	                 			for(String s:subword){						//关键词
	                 				if(!text.contains(s)){
	                 					break;
	                 					}
	                 				}
	                 			if(result.containsKey(info[1]+"\t"+word+"\t"+info[3])){
	                 				Map temp=result.get(info[1]+"\t"+word+"\t"+info[3]);
	                 				if(!temp.containsKey(info[3])){
	                 					temp.put(info[0], "");	
	                 				}
	                 				result.put(info[1]+"\t"+word+"\t"+info[3], temp);
	                 			}else{
	                 				Map temp=new HashMap();
	                 				temp.put(info[0], "");
	                 				result.put(info[1]+"\t"+word+"\t"+info[3], temp);		//key:事件  关键词组  info[3]  
	                 																		//value:key:微博MID  value:""
	                 			}
	                       	    
	                         }
	                	}
	            }
	      
			}
			br0.close();
        }
        
        
    Iterator<Entry<String, Map>> it2= result.entrySet().iterator();
		
		while (it2.hasNext()) {
			Entry<String, Map> entry = it2.next();
			
			IOTool.write("E:\\sina\\zhongkeyuan\\rt_p.txt", entry.getKey() + "\t" + entry.getValue().size(), true, "utf-8");

		}
	}
	

}
