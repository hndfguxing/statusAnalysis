package Mood;

import Util.IOTool;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

/**
 * 统计每月、周、天、小时发表的微博数量以及包含特定情绪微博的比例
 */
public class MergeResult {
    SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:00:00");
    List<String> keywordList = new ArrayList<String>();
    List<String> timeMotionList = new ArrayList<String>();
    List<String> timeTweetList = new ArrayList<String>();
    String outputDir = "E:\\sina\\nankai\\test";
    
    public static void main(String[] args)
    {
        //MergeResult mergeResult = new MergeResult();
        //mergeResult.userActiveTime();
        //mergeResult.test();
        MergeResult mergeResult = new MergeResult();
        mergeResult.merge2Month();
        mergeResult.merge2Week();
        mergeResult.merge2Day();
        mergeResult.merge2Hour();
        //mergeResult.statisRegisterTime();
    }
    public MergeResult()
    {
    	 keywordList = readFile("E:\\sina\\motion");
         timeMotionList = readFile("E:\\sina\\nankai\\Timemoodcount_tweet");
         timeTweetList = readFile("E:\\sina\\nankai\\TimeTweetCount_ori");
         this.outputDir = outputDir;
    }
    public MergeResult(String keywordDir,String timeTweetDir,String timeMotionDir,String outputDir)
    {
        keywordList = readFile(keywordDir);
        timeMotionList = readFile(timeMotionDir);
        timeTweetList = readFile(timeTweetDir);
        this.outputDir = outputDir;
    }
    public void test()
    {
        List<String> uidList = new ArrayList<String>();
        Set<String> edgeSet = new HashSet<String>();
        List<String> dataList = readFile("D:\\左右派用户关系网络\\node_all.csv");
        for(String data:dataList)
        {
            String[] list = data.split("\t");
            uidList.add(list[0]);
        }
        dataList = readFile("D:\\左右派用户关系网络\\edge_all.csv");
        for(String data:dataList)
        {
            String[] list = data.split("\t");
            if(uidList.contains(list[0])&& uidList.contains(list[1])&& !edgeSet.contains(data))
            {
                edgeSet.add(data);
            }
        }
        System.out.println(edgeSet.size());
                
    }
    public void userActiveTime()
    {
        Map<String,Integer> dayCountMap = new HashMap<String,Integer>();
        Map<String,Integer> weekCountMap = new HashMap<String,Integer>();
        Map<String,Integer> yearCountMap = new HashMap<String,Integer>();
        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:00:00");
        SimpleDateFormat inputFormat1 = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat inputFormat2 = new SimpleDateFormat("yyyy-MM-00");
        ArrayList<String> dayList = new ArrayList<String>();
        ArrayList<String> weekList = new ArrayList<String>();
        ArrayList<String> yearList = new ArrayList<String>();
        String time = null;
        try {
            for(String data:timeTweetList)
            {
                String[] list = data.split("\t");
                if(list.length>=2)
                {
                    time = inputFormat1.format(inputFormat.parse(list[0]));
                    if(!dayList.contains(time))
                    {
                        dayList.add(time);
                    }
                    Date date = inputFormat.parse(list[0]);
                    int day = date.getDay()-1;
                    if(day==-1) {
                        day = 6;
                    }
                    long timeL = date.getTime()-day*24*3600*1000;
                    date = new Date(timeL);
                    if(!weekList.contains(inputFormat1.format(date)))
                    {
                        weekList.add(inputFormat1.format(date));
                    }
                    time = inputFormat2.format(inputFormat.parse(list[0]));
                    if(!yearList.contains(time))
                    {
                        yearList.add(time);
                    }
                }
            }
            }catch (ParseException e) {
                // TODO Auto-generated catch block
                //e.printStackTrace();
            }
            String startTime1;
            String endTime1;
            List<String> userActiveList = readFile("Z:\\code\\data\\motionwordStatis20130112\\UserActiveTime");
            for(String data:userActiveList)
            {
                String[] list = data.split("\t");
                if(list.length>=3)
                {
                    String startTime = list[1];
                    String endTime = list[2];
                    for(String date:dayList)
                    {
                        if(date.compareTo(startTime)>=0&& date.compareTo(endTime)<=0)
                        {
                            if(dayCountMap.containsKey(date))
                            {
                                int temp = dayCountMap.get(date);
                                dayCountMap.put(date, ++temp);
                            }else
                            {
                                dayCountMap.put(date, new Integer(1));
                            }
                        }
                    }
                    Date startDate;
                    try {
                        startDate = inputFormat.parse(startTime);
                        int day = startDate.getDay()-1;
                        if(day==-1) {
                            day = 6;
                        }
                        long timeL = startDate.getTime()-day*24*3600*1000;
                        startDate = new Date(timeL);
                        startTime1 = inputFormat1.format(startDate);
                        Date endDate = inputFormat.parse(endTime);
                        day = startDate.getDay()-1;
                        if(day==-1) {
                            day = 6;
                        }
                        timeL = startDate.getTime()-day*24*3600*1000;
                        endDate = new Date(timeL);
                        endTime1 = inputFormat1.format(endDate);
                        for(String date:weekList)
                        {
                            
                            if(date.compareTo(startTime1)>=0&& date.compareTo(endTime1)<=0)
                            {
                                if(weekCountMap.containsKey(date))
                                {
                                    int temp = weekCountMap.get(date);
                                    weekCountMap.put(date, ++temp);
                                }else
                                {
                                    weekCountMap.put(date, new Integer(1));
                                }
                            }
                        }
                    } catch (ParseException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    
                    
                    try {
                        startTime1 = inputFormat2.format(inputFormat.parse(startTime));
                        endTime1 = inputFormat2.format(inputFormat.parse(endTime));
                        for(String date:yearList)
                        {
                            
                            if(date.compareTo(startTime1)>=0&& date.compareTo(endTime1)<=0)
                            {
                                if(yearCountMap.containsKey(date))
                                {
                                    int temp = yearCountMap.get(date);
                                    yearCountMap.put(date, ++temp);
                                }else
                                {
                                    yearCountMap.put(date, new Integer(1));
                                }
                            }else
                            {
                                //System.out.println(date+"\t"+startTime1+"\t"+endTime1);
                            }
                        }
                    } catch (ParseException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    
                    
                            
                }
            }
            
        
        List<String> resultList= new ArrayList<String>();
        Iterator iter = dayCountMap.entrySet().iterator();
        while(iter.hasNext())
        {
            Entry<String,Integer> entry = (Entry<String,Integer>) iter.next();
            String key = entry.getKey();
            int value = entry.getValue();
            resultList.add(key+"\t"+value);
        }
        Collections.sort(resultList);
        IOTool.write(".\\data\\motion\\userActive_day", resultList, false, "utf8");
        resultList.clear();
        iter = weekCountMap.entrySet().iterator();
        while(iter.hasNext())
        {
            Entry<String,Integer> entry = (Entry<String,Integer>) iter.next();
            String key = entry.getKey();
            int value = entry.getValue();
            resultList.add(key+"\t"+value);
        }
        Collections.sort(resultList);
        IOTool.write(".\\data\\motion\\userActive_week", resultList,false,"utf8");
        resultList.clear();
        iter = yearCountMap.entrySet().iterator();
        while(iter.hasNext())
        {
            Entry<String,Integer> entry = (Entry<String,Integer>) iter.next();
            String key = entry.getKey();
            int value = entry.getValue();
            resultList.add(key+"\t"+value);
        }
        Collections.sort(resultList);
        IOTool.write(".\\data\\motion\\userActive_year", resultList,false,"utf8");
        resultList.clear();
    }
    public void statisRegisterTime()
    {
        List<String> dateList = new ArrayList<String>();
        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:00:00");
        SimpleDateFormat inputFormat1 = new SimpleDateFormat("yyyy-MM-dd");
        Map<String,Integer> timeCountMap = new HashMap<String,Integer>();
        List<String> dataList = readFile("Z:\\code\\data\\motionwordStatis20130112\\UserRegisterTime");
        for(String data:dataList)
        {
            String[] list = data.split("\t");
            if(list.length>=2)
            {
                String time = list[0];
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
        }
        List<String> resultList = new ArrayList<String>();
        //write to file
        
        Iterator iterator = timeCountMap.entrySet().iterator();
        while (iterator.hasNext())
        {
            Entry<String,Integer> entry =  (Entry<String, Integer>) iterator.next();
            String key = entry.getKey();
            int value = entry.getValue();
            resultList.add(key+"\t"+value);
        }
        Collections.sort(resultList);
        IOTool.write(".\\data\\motion\\userRegisterTime.txt", resultList,false,"utf8");    
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
    
    /**
     * 统计每小时发表的微博数量以及包含特定情绪微博的比例
     */
    public void merge2Hour()
    {
        List<String> dateList = new ArrayList<String>();
        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:00:00");
        SimpleDateFormat inputFormat1 = new SimpleDateFormat("yyyy-MM-dd HH:00:00");
        Map<String,Integer> timeCountMap = new HashMap<String,Integer>();
        try {
            for(String data:timeTweetList)
            {
                String[] list = data.split("\t");
                if(list.length>=2)
                {
                    Date date = inputFormat.parse(list[0]);
                    if(!dateList.contains(inputFormat1.format(date)))
                    {
                        dateList.add(inputFormat1.format(date));
                    }
                    String time = inputFormat1.format(date)+"\t"+ (date.getHours()+1);
                    
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
            }
            Map<String,Integer> timeMotionCountMap = new HashMap<String,Integer>();
            for(String data:timeMotionList)
            {
                String[] list = data.split("\t");
                if(list.length>=3)
                {
                    Date date = inputFormat.parse(list[0]);
                    String time = inputFormat1.format(date)+"\t"+ (date.getHours()+1);
                    String keyword = list[1];
                    int count = Integer.parseInt(list[2]);
                    if(timeMotionCountMap.containsKey(time+"\t"+keyword))
                    {
                        int temp = timeMotionCountMap.get(time+"\t"+keyword);
                        temp += count;
                        timeMotionCountMap.put(time+"\t"+keyword, temp);
                    }else
                    {
                        timeMotionCountMap.put(time+"\t"+keyword, new Integer(count));
                    }
                }
            }
            List<String> resultList = new ArrayList<String>();
            Collections.sort(dateList);
            int allCount = 0;
            
            for(String date:dateList)
            {
                for(int hour=1;hour<25;hour++)
                {
                    StringBuffer result = new StringBuffer();
                    if(timeCountMap.containsKey(date+"\t"+hour))
                    {
                        allCount = timeCountMap.get(date+"\t"+hour);
                    }else
                    {
                        continue;
                    }
                    result.append(date+"\t"+hour+"\t"+timeCountMap.get(date+"\t"+hour)+"\t");
                    for(String keyword:keywordList)
                    {
                        if(timeMotionCountMap.containsKey(date+"\t"+hour+"\t"+keyword))
                        {
                            double temp = ((double)timeMotionCountMap.get(date+"\t"+hour+"\t"+keyword))/allCount;
                            //int temp = timeMotionCountMap.get(date+"\t"+hour+"\t"+keyword);
                            result.append(temp+"\t");
                        }else
                        {
                            result.append(0+"\t");
                        }
                    }
                    resultList.add(result.toString());
                }
            }
            StringBuffer keywordBuffer = new StringBuffer();
            keywordBuffer.append("日期"+"\t"+"小时"+"\t"+"微博总数"+"\t");
            for(String keyword:keywordList)
            {
                keywordBuffer.append(keyword+"\t");
            }
            IOTool.write(this.outputDir+File.separator+"Hour.txt", keywordBuffer.toString(),false,"utf8");
            IOTool.write(this.outputDir+File.separator+"Hour.txt", resultList,true,"utf8");
            resultList.clear();
            keywordBuffer.delete(0, keywordBuffer.length());
            timeMotionCountMap.clear();
            timeCountMap.clear();
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    /**
     * 统计每天发表的微博数量以及包含特定情绪微博的比例
     */
    public void merge2Day()
    {
        List<String> dateList = new ArrayList<String>();
        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:00:00");
        SimpleDateFormat inputFormat1 = new SimpleDateFormat("yyyy-MM-dd");
        Map<String,Integer> timeCountMap = new HashMap<String,Integer>();
        try {
            for(String data:timeTweetList)
            {
                String[] list = data.split("\t");
                if(list.length>=2)
                {
                    Date date = inputFormat.parse(list[0]);
                    if(!dateList.contains(inputFormat1.format(date)))
                    {
                        dateList.add(inputFormat1.format(date));
                    }
                    String time = inputFormat1.format(inputFormat.parse(list[0]));
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
            }
            Map<String,Integer> timeMotionCountMap = new HashMap<String,Integer>();
            for(String data:timeMotionList)
            {
                String[] list = data.split("\t");
                if(list.length>=3)
                {
                    String time = inputFormat1.format(inputFormat.parse(list[0]));
                    String keyword = list[1];
                    int count = Integer.parseInt(list[2]);
                    if(timeMotionCountMap.containsKey(time+"\t"+keyword))
                    {
                        int temp = timeMotionCountMap.get(time+"\t"+keyword);
                        temp += count;
                        timeMotionCountMap.put(time+"\t"+keyword, temp);
                    }else
                    {
                        timeMotionCountMap.put(time+"\t"+keyword, new Integer(count));
                    }
                }
            }
            //write to file
            List<String> resultList = new ArrayList<String>();
            Collections.sort(dateList);
            for(String date:dateList)
            {
                int allCount = 0;
                StringBuffer result = new StringBuffer();
                if(timeCountMap.containsKey(date))
                {
                    allCount = timeCountMap.get(date);
                }else
                {
                    continue;
                }
                result.append(date+"\t"+timeCountMap.get(date)+"\t");
                for(String keyword:keywordList) 
                {
                    if(timeMotionCountMap.containsKey(date+"\t"+keyword))
                    {
                        double temp = ((double)timeMotionCountMap.get(date+"\t"+keyword))/allCount;
                        //int temp = timeMotionCountMap.get(date+"\t"+keyword);
                        result.append(temp+"\t");
                    }else
                    {
                        result.append(0+"\t");
                    }
                }
                resultList.add(result.toString());
            }
            StringBuffer keywordBuffer = new StringBuffer();
            keywordBuffer.append("日期"+"\t"+"微博总数"+"\t");
            for(String keyword:keywordList)
            {
                keywordBuffer.append(keyword+"\t");
            }
            IOTool.write(this.outputDir+File.separator+"day.txt", keywordBuffer.toString(),false,"utf8");
            keywordBuffer.delete(0, keywordBuffer.length());
            IOTool.write(this.outputDir+File.separator+"day.txt", resultList,true,"utf8");
            resultList.clear();
            timeMotionCountMap.clear();
            timeCountMap.clear();
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    /**
     * 统计每周发表的微博数量以及包含特定情绪微博的比例
     */
    public void merge2Week()
    {
        List<String> dateList = new ArrayList<String>();
        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:00:00");
        SimpleDateFormat inputFormat1 = new SimpleDateFormat("yyyy-MM-dd");
        Map<String,Integer> timeCountMap = new HashMap<String,Integer>();
        try {
            for(String data:timeTweetList)
            {
                String[] list = data.split("\t");
                if(list.length>=2)
                {
                    Date date = inputFormat.parse(list[0]);
                    int day = date.getDay()-1;
                    if(day==-1) {
                        day = 6;
                    }
                    long timeL = date.getTime()-day*24*3600*1000;		//将星期几换为星期一
                    date = new Date(timeL);
                    if(!dateList.contains(inputFormat1.format(date)))
                    {
                        dateList.add(inputFormat1.format(date));
                    }
                    String time = inputFormat1.format(date);
                    int count = Integer.parseInt(list[1]);
                    if(timeCountMap.containsKey(time))					//统计每周发表的微博数量
                    {
                        int temp = timeCountMap.get(time);
                        temp += count;
                        timeCountMap.put(time, temp);
                    }else
                    {
                        timeCountMap.put(time, new Integer(count));
                    }
                }
            }
            Map<String,Integer> timeMotionCountMap = new HashMap<String,Integer>();for(String data:timeMotionList)
            {
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
                    String time = inputFormat1.format(date);
                    String keyword = list[1];
                    int count = Integer.parseInt(list[2]);
                    if(timeMotionCountMap.containsKey(time+"\t"+keyword))		//统计每周发表的包含特定情绪的微博的数量
                    {
                        int temp = timeMotionCountMap.get(time+"\t"+keyword);
                        temp += count;
                        timeMotionCountMap.put(time+"\t"+keyword, temp);
                    }else
                    {
                        timeMotionCountMap.put(time+"\t"+keyword, new Integer(count));
                    }
                }
            }
            //write to file
            List<String> resultList = new ArrayList<String>();
            Collections.sort(dateList);
            for(String date:dateList)
            {
                int allCount = 0;
                StringBuffer result = new StringBuffer();
                if(timeCountMap.containsKey(date))
                {
                    allCount = timeCountMap.get(date);
                }else
                {
                    continue;
                }
                result.append(date+"\t"+timeCountMap.get(date)+"\t");
                for(String keyword:keywordList) 
                {
                    if(timeMotionCountMap.containsKey(date+"\t"+keyword))
                    {
                        double temp = ((double)timeMotionCountMap.get(date+"\t"+keyword))/allCount;		//计算每周发表的包含特定情绪微博的比例
                        //int temp = timeMotionCountMap.get(date+"\t"+keyword);
                        result.append(temp+"\t");
                    }else
                    {
                        result.append(0+"\t");
                    }
                }
                resultList.add(result.toString());
            }
            StringBuffer keywordBuffer = new StringBuffer();
            keywordBuffer.append("日期"+"\t"+"微博总数"+"\t");
            for(String keyword:keywordList)
            {
                keywordBuffer.append(keyword+"\t");
            }
            IOTool.write(this.outputDir+File.separator+"week.txt", keywordBuffer.toString(),false,"utf8");
            keywordBuffer.delete(0, keywordBuffer.length());
            IOTool.write(this.outputDir+File.separator+"week.txt", resultList,true,"utf8");
            resultList.clear();
            timeMotionCountMap.clear();
            timeCountMap.clear();
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    /**
     * 统计每个月里发表的微博数量以及包含特定情绪微博的比例
     */
    public void merge2Month()
    {
        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:00:00");
        Map<String,Integer> timeCountMap = new HashMap<String,Integer>();
        try {
        	
        	//统计每个月里发表微博的数量,存入temiCountMap中
            for(String data:timeTweetList)
            {
                String[] list = data.split("\t");
                if(list.length>=2)
                {
                    Date date = inputFormat.parse(list[0]);
                    String time = (date.getYear()+1900)+"\t"+(date.getMonth()+1);		//年月
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
                }else
                {
                    System.out.println("error");
                }
            }
            
            //统计每个月里发表的包含特定情绪微博的数量,存入 timeMotionCountMap中
            Map<String,Integer> timeMotionCountMap = new HashMap<String,Integer>();
            for(String data:timeMotionList)
            {
                String[] list = data.split("\t");		//时间  情绪关键字  数量
                if(list.length>=3)
                {
                    Date date = inputFormat.parse(list[0]);
                    String time = (date.getYear()+1900)+"\t"+(date.getMonth()+1);
                    String keyword = list[1];
                    int count = Integer.parseInt(list[2]);
                    if(timeMotionCountMap.containsKey(time+"\t"+keyword))
                    {
                        int temp = timeMotionCountMap.get(time+"\t"+keyword);
                        temp += count;
                        timeMotionCountMap.put(time+"\t"+keyword, temp);
                    }else
                    {
                        timeMotionCountMap.put(time+"\t"+keyword, new Integer(count));
                    }
                }
            }
            
            //write to file
            List<String> resultList = new ArrayList<String>();
            for(int year= 2009;year<2014;year++)
            {
                for(int month=1;month<13;month++)
                {
                    int allCount = 0;
                    StringBuffer result = new StringBuffer();
                    if(timeCountMap.containsKey(year+"\t"+month))
                    {
                        allCount = timeCountMap.get(year+"\t"+month);		//存放这个月发表微博的总数量
                    }else
                    {
                        continue;
                    }
                    result.append(year+"\t"+month+"\t"+timeCountMap.get(year+"\t"+month)+"\t");
                    for(String keyword:keywordList) 
                    {
                        if(timeMotionCountMap.containsKey(year+"\t"+month+"\t"+keyword))
                        {
                            double temp = ((double)timeMotionCountMap.get(year+"\t"+month+"\t"+keyword))/allCount;		//计算这个月包含特定情绪微博的比例
                            //int temp = timeMotionCountMap.get(year+"\t"+month+"\t"+keyword);
                            result.append(temp+"\t");
                        }else
                        {
                            result.append(0+"\t");
                        }
                    }
                    resultList.add(result.toString());
                }
            }
            StringBuffer keywordBuffer = new StringBuffer();
            keywordBuffer.append("年"+"\t"+"月"+"\t"+"微博总数"+"\t");
            for(String keyword:keywordList)
            {
                keywordBuffer.append(keyword+"\t");
            }
            IOTool.write(this.outputDir+File.separator+"year.txt", keywordBuffer.toString(),false,"utf8");		//将"年"+"\t"+"月"+"\t"+"微博总数"+"\t"写入以year命名的文件
            keywordBuffer.delete(0, keywordBuffer.length());
            IOTool.write(this.outputDir+File.separator+"year.txt", resultList,true,"utf8");
            resultList.clear();
            timeMotionCountMap.clear();
            timeCountMap.clear();
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
