package MentionEventCount;


import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class CombineEventCount {
	
    public static void main(String[] args) {
        CombineEventCount getUserId = new CombineEventCount();
        getUserId.getUserEventCount();
    }
    
	/**
     * 合并MentionEventCount类输出的相同的用户项
     * 
     * 输入：微博作者UID  特定事件  提及特定事件的次数
     * 输出：提及特定事件的总次数  微博作者UID
     */
    public void getUserEventCount() {
    	
        Map<String,Long> eventCountMap = new HashMap<String,Long>();
        FileInputStream fis;
        InputStreamReader isr;
        BufferedReader br;
        String line = null;
        File[] flist = new File("Z:\\code\\data\\political\\UserEventCount").listFiles();
        try {
            for(File f:flist) {
                fis = new FileInputStream(f);
                isr = new InputStreamReader(fis,"utf-8");
                br = new BufferedReader(isr);

                //data format : UID \t event \t count
                while((line = br.readLine()) != null)
                {
                    String[] list = line.split("\t");
                    if(eventCountMap.containsKey(list[0]))
                    {
                        long temp = eventCountMap.get(list[0]);
                        temp += Long.parseLong(list[2]);
                        eventCountMap.put(list[0], temp);
                    }else
                    {
                        eventCountMap.put(list[0], Long.parseLong(list[2]));
                    }
                }
                br.close();
                isr.close();
                fis.close();
            }
            
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } 
        
        Iterator<Entry<String, Long>> iterator = eventCountMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String,Long> entry = (Map.Entry<String,Long>) iterator.next();
            String key = entry.getKey();
            long value = entry.getValue();
            if(value>1) {
                write(".\\data\\uidCount", value + "\t" + key);
            }
        }
        
    }//getUserEventCount method end
    
	public static boolean write(String filepath,String str) {
	    OutputStreamWriter osw = null;
        FileOutputStream fileos = null;
        BufferedWriter bw = null;
        try {
            fileos = new FileOutputStream(filepath, true);
            osw = new OutputStreamWriter(fileos,"utf-8");
            bw = new BufferedWriter(osw);
            if(!str.equals("")) {
                bw.append(str);
                bw.newLine();
            }
            bw.close();
            osw.close();
            fileos.close();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } 
    }
	
}
