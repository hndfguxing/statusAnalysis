package Mood;

import Util.IOTool;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 统计某个月里发表的微博数量
 */
public class Merge {
	static List<String> timeTweetList = new ArrayList<String>();
	
	public Merge()
	{
		timeTweetList = readFile("E:\\sina\\nankai\\TimeTweetCount_rt");
	}
	
	/**
	 * 将文件夹中的每个文件以行为单位读入到dataList中
	 */
	public List<String> readFile(String inputDir)
    {
        List<String> dataList = new ArrayList<String>();
        FileInputStream fis;
        InputStreamReader isr;
        BufferedReader br;
        String line = null;
        File[] flist = new File(inputDir).listFiles();
        try {
            for(File f:flist)
            {
                System.out.println(f.getName());
                fis = new FileInputStream(f);
                isr = new InputStreamReader(fis,"utf8");
                br = new BufferedReader(isr);
                while((line = br.readLine()) != null)
                {
                    dataList.add(line);
                }
                br.close();
                isr.close();
                fis.close();
            }
        }catch (FileNotFoundException e) {
         // TODO Auto-generated catch block
            System.err.println(e.getMessage());
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            System.err.println(e.getMessage());
            e.printStackTrace();
        }  
        catch (IOException e) {
            // TODO Auto-generated catch block
            System.err.println(e.getMessage());
            e.printStackTrace();
        }
        return dataList;
    }

	 public static void main(String[] args)
	 {
		    Merge merge=new Merge();
		    merge.merge2month();
	      
	    
	 }
	 public void merge2week()
	 {
	        List<String> dateList = new ArrayList<String>();
	        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:00:00");
	        SimpleDateFormat inputFormat1 = new SimpleDateFormat("yyyy-MM-dd");
	        Map<String,Integer> timeCountMap = new HashMap<String,Integer>();
	      
	            for(String data:timeTweetList)
	            {
	             try {
	                String[] list = data.split("\t");
	                if(list.length>=2)
	                {
	                    Date date = inputFormat.parse(list[0]);
	                    int day = date.getDay()-1;
	                    if(day==-1) {
	                        day = 6;
	                    }
	                    long timeL = date.getTime()-day*24*3600*1000;
	                    date = new Date(timeL);
	                    if(!dateList.contains(inputFormat1.format(date)))
	                    {
	                        dateList.add(inputFormat1.format(date));
	                    }
	                    String time = inputFormat1.format(date);
	                    int count = Integer.parseInt(list[1]);
	                    if(timeCountMap.containsKey(time))
	                    {
	                        int temp = timeCountMap.get(time);
	                        temp += count;
	                        timeCountMap.put(time, temp);
	                    }else
	                    {
	                        timeCountMap.put(time, new Integer(count));
	                    }
	                }
	            } catch (ParseException e) {
		            // TODO Auto-generated catch block
		            continue;
		        }
	            }
	    
	            //write to file
	            List<String> resultList = new ArrayList<String>();
	            Collections.sort(dateList);
	            for(String date:dateList)
	            {
	               
	                if(timeCountMap.containsKey(date))
	                {
	                	IOTool.write("E:\\sina\\nankai\\rt_week.txt", date + "\t" + timeCountMap.get(date), true, "utf8");
	                }
	                
	                
	            }
	            
	       
	    }
	 public void merge2month()
	 {
		 List<String> dateList = new ArrayList<String>();
	        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:00:00");
	        SimpleDateFormat inputFormat1 = new SimpleDateFormat("yyyy-MM");
	        Map<String,Integer> timeCountMap = new HashMap<String,Integer>();
	        
	            for(String data:timeTweetList)
	            {
	            	try {
	                String[] list = data.split("\t");
	                if(list.length>=3)
	                {
	                	
	                    Date date = inputFormat.parse(list[0]);								
	                    if(!dateList.contains(inputFormat1.format(date)))					//日期存入dateList中
	                    {
	                        dateList.add(inputFormat1.format(date));
	                    }
	                    String time = inputFormat1.format(inputFormat.parse(list[0]));
	                    int count = Integer.parseInt(list[1]);
	                    if(timeCountMap.containsKey(time))									//日期年月存入temeCountMap的key中
	                    {
	                        int temp = timeCountMap.get(time);
	                        temp += count;
	                        timeCountMap.put(time, temp);									//value为对应日期的微博数
	                    }else
	                    {
	                        timeCountMap.put(time, new Integer(count));
	                    }
	                }
	            } catch (ParseException e) {
		            // TODO Auto-generated catch block
		            continue;
		        }
	            }
	            Map<String,Integer> timeMotionCountMap = new HashMap<String,Integer>();
	      
	            //write to file
	            List<String> resultList = new ArrayList<String>();
	            Collections.sort(dateList);
	            for(String date:dateList)
	            {
	                StringBuffer result = new StringBuffer();
	                if(timeCountMap.containsKey(date))
	                {
	                	IOTool.write("E:\\sina\\nankai\\month.txt", date+"\t"+timeCountMap.get(date),true,"utf8");
	                }
	            }
	           
	    }
	   
}
