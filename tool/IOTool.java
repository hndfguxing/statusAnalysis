package Util;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
* 一些常用的读写方法
*/
public class Tool {
    
	public static ArrayList<String> readFile(String filepath,String encode) {
        ArrayList<String> list = new ArrayList<String>();
        FileInputStream fis;
        InputStreamReader isr;
        BufferedReader br;
        String line = null;
        try {
            fis = new FileInputStream(filepath);
            isr = new InputStreamReader(fis,encode);
            br = new BufferedReader(isr);
            while((line = br.readLine()) != null) {
                list.add(line);
            }
            br.close();
            isr.close();
            fis.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }
	
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
	
	public static boolean write(String filepath, String str ,boolean isAppend,String encode) {
        OutputStreamWriter osw = null;
        FileOutputStream fileos = null;
        BufferedWriter bw = null;
        try {
            fileos = new FileOutputStream(filepath, isAppend);
            osw = new OutputStreamWriter(fileos,encode);
            bw = new BufferedWriter(osw);
            bw.append(str);
            bw.newLine();
            bw.close();
            osw.close();
            fileos.close();
            return true;
        }
        catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }
    
	public static boolean write(String filepath, List<String> list) {
	    OutputStreamWriter osw = null;
        FileOutputStream fileos = null;
        BufferedWriter bw = null;
        try {
            fileos = new FileOutputStream(filepath, true);
            osw = new OutputStreamWriter(fileos,"GBK");
            bw = new BufferedWriter(osw);
            for (String s : list) {
                if(!s.equals("")) {
                    bw.append(s);
                    bw.newLine();
                }
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
	
	public static boolean write(String filepath, List<String> list,boolean isAppend,String encode) {
        OutputStreamWriter osw = null;
        FileOutputStream fileos = null;
        BufferedWriter bw = null;
        try {
            fileos = new FileOutputStream(filepath, isAppend);
            osw = new OutputStreamWriter(fileos,encode);
            bw = new BufferedWriter(osw);
            for (String s : list)
            {
                bw.append(s);
                bw.newLine();
            }
            bw.close();
            osw.close();
            fileos.close();
            return true;
        }
        catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }
	
	public static boolean write(String filepath, Map<String,Long> resultMap) {
		Iterator<Entry<String, Long>> iterator = resultMap.entrySet().iterator();
        List<String> resultList = new ArrayList<String>();
        while (iterator.hasNext()) {
			Entry<String, Long> entry = (Map.Entry<String,Long>) iterator.next();
			resultList.add(entry.getKey()+"\t"+entry.getValue());
		}
        OutputStreamWriter osw = null;
        FileOutputStream fileos = null;
        BufferedWriter bw = null;
        try {
            fileos = new FileOutputStream(filepath, true);
            osw = new OutputStreamWriter(fileos,"GBK");
            bw = new BufferedWriter(osw);
            for (String s : resultList) {
                if(!s.equals("")) {
                    bw.append(s);
                    bw.newLine();
                }
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
	
    public static String removeEol(String text) {
		if(text == null) {
			return text;
		}
		if(text.contains("\n")) {
			text = text.replaceAll("\n", "");
		}
		if(text.contains("\r")) {
			text = text.replaceAll("\r", "");
		}
		if(text.contains("\n\r")) {
			text = text.replaceAll("\n\r", "");
		}
		if(text.contains("\r\n")) {
			text = text.replaceAll("\r\n", "");
		}
		return text;
	}
    
    public static boolean deleteFile(File file) {
        try {
            if (file.exists()) {
                file.delete();
                System.out.println("delete："+file.getName());
                return true;
            }
        } catch (Exception e) {
            System.out.println("delete failed："+file.getName());
            e.printStackTrace();
        }
        return false;
    }
    
}
