package Mood;

import Util.IOTool;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class MergeMood {
	static List<String> timeTweetList = new ArrayList<String>();
	static List<String> keyword = new ArrayList<String>();
	
	public MergeMood()
	{
		//timeTweetList = readFile("E:\\sina\\nankai\\TimeMoodCount_TWRT");
		//keyword=readFile("E:\\sina\\motion");
	}
	
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

	 public static void main(String[] args) throws ParseException, IOException
	 {
		 MergeMood merge=new MergeMood();
		 merge.merge2hour();
	      
	    
	 }
	 
	 public void merge2hour() throws ParseException, IOException
	 {
		 List<String> dateList = new ArrayList<String>();
	     SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:00:00");
	     Map<String,String> timeCountMap = new HashMap<String,String>();
	     FileInputStream fis = null;
	        InputStreamReader isr = null;
	        BufferedReader br = null;
	        String line = null;
	        File[] flist = new File("E:\\sina\\nankai\\Timemoodcount_RT").listFiles();
	        
	            for(File f:flist)
	            {
	                System.out.println(f.getName());
	                fis = new FileInputStream(f);
	                isr = new InputStreamReader(fis,"utf8");
	                br = new BufferedReader(isr);
	                while((line = br.readLine()) != null)
	                {
	                	String[] list = line.split("\t");
	                	 if(list.length>=2)
	 	                {
	                		 //System.out.println("haha");
	                		 try{
	 	                    Date date = inputFormat.parse(list[0]);
	 	                   IOTool.write("E:\\sina\\nankai\\Timemoodcount_RT_clean\\" + f.getName(), line, true, "utf8");}
	                		 catch(Exception e){
	                			 continue;
	                		 }
	 	                   /* if(!dateList.contains(inputFormat.format(date)))
	 	                    {
	 	                        dateList.add(inputFormat.format(date));
	 	                    }
	 	                    if(!timeCountMap.containsKey(inputFormat.format(date)))
	 	                    {
	 	                        
	 	                        timeCountMap.put(inputFormat.format(date), data);
	 	                    }*/
	 	                     
	                }
	               
	            }
	     
	       
	              
	               
	            
	      }
	     br.close();
	                isr.close();
	                fis.close();
	           
	      //Collections.sort(dateList);
	          
	      /*for(String date:dateList)
	      {
	          if(timeCountMap.containsKey(date))
	          {
	               Tool.write("E:\\sina\\nankai\\ori_mood_hour.txt", timeCountMap.get(date),true,"utf8"); 
	          }
	      }*/
	            
	       
	    }
	 public void merge2week()
	 {
		 Collections.sort(keyword);
		 String result="time";
		 for(String key:keyword)
         {
            result+="\t"+key;
             
         }
		 IOTool.write("E:\\sina\\nankai\\ori_mood_week.txt", result,true,"utf8");
	     
		 List<String> dateList = new ArrayList<String>();
	     SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:00:00");
	     SimpleDateFormat inputFormat1 = new SimpleDateFormat("yyyy-MM-dd");
	     Map<String,Integer> timeCountMap = new HashMap<String,Integer>();
	      
	     for(String data:timeTweetList)
	     {
	         try {
	                String[] list = data.split("\t");
	                if(list.length>=3)
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
	                    int count = Integer.parseInt(list[2]);
	                    if(timeCountMap.containsKey(time+"\t"+list[1]))
	                    {
	                        int temp = timeCountMap.get(time+"\t"+list[1]);
	                        temp += count;
	                        timeCountMap.put(time+"\t"+list[1], temp);
	                    }else
	                    {
	                        timeCountMap.put(time+"\t"+list[1], new Integer(count));
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
	            Collections.sort(keyword);
	            for(String date:dateList)
	            {
	            	String s=date;
	               for(String key:keyword){
	                if(timeCountMap.containsKey(date+"\t"+key))
	                {
	                	s+="\t"+timeCountMap.get(date+"\t"+key);
	                }else{
	                	s+="\t"+0;
	                }
	               }
	               IOTool.write("E:\\sina\\nankai\\ori_mood_week.txt", s,true,"utf8"); 
	            }
	            
	       
	    }
	 public void merge2month()
	 {
		 Collections.sort(keyword);
		 String result="time";
		 for(String key:keyword)
         {
            result+="\t"+key;
             
         }
		 IOTool.write(".//twrt_mood_month.txt", result,true,"utf8");
		 
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
	                    if(!dateList.contains(inputFormat1.format(date)))
	                    {
	                        dateList.add(inputFormat1.format(date));
	                    }
	                    String time = inputFormat1.format(date);
	                    int count = Integer.parseInt(list[2]);
	                    if(timeCountMap.containsKey(time+"\t"+list[1]))
	                    {
	                        int temp = timeCountMap.get(time+"\t"+list[1]);
	                        temp += count;
	                        timeCountMap.put(time+"\t"+list[1], temp);
	                    }else
	                    {
	                        timeCountMap.put(time+"\t"+list[1], new Integer(count));
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
	            Collections.sort(keyword);
	            for(String date:dateList)
	            {
	            	String s=date;
	               for(String key:keyword){
	                if(timeCountMap.containsKey(date+"\t"+key))
	                {
	                	s+="\t"+timeCountMap.get(date+"\t"+key);
	                }else{
	                	s+="\t"+0;
	                }
	               }
	               IOTool.write(".//twlkrt_mood_month.txt", s,true,"utf8"); 
	            }
	            resultList.clear();
	            timeTweetList.clear();
	            keyword.clear();
	            timeCountMap.clear();
	    }
	   
}
