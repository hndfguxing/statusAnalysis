package MentionEventCount;

import Util.IOTool;
import weibo4j.WeiboException;
import weibo4j.model.Status;
import weibo4j.model.StatusWapper;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GetEventStatus {
	
    ArrayList<String> keywordList = new ArrayList<String>();
	
    public static void main(String args[])
    {
        GetEventStatus testinLocal = new GetEventStatus();
        testinLocal.test();
        StringBuffer result = new StringBuffer();
        result.append("test  000a");
        result.delete(0, result.length());
        System.out.println(result.toString());
        
    }
	
	/**
    * 找出提及事件"方舟子VS韩寒"的所有微博
    * 
    * 输入：微博集合、关键行列表（每一行以/t分隔的2个关键字，list[0]为事件，list[1]为事件关键字）
    * 输出：提及事件"方舟子VS韩寒"的微博及转发微博
    */
    public void test() {
    
        readKeyword();			//添加文件每一行到keywordList中  每一行是以/t分隔符的两个关键字
        FileInputStream fis;
        InputStreamReader isr;
        BufferedReader br;
        String line = null;
        File[] fileList = null;
        String text = null;
        String rtText = null;
        List<Status> statusList = new ArrayList<Status>();
        int allCount = 0;		//存储所有读入的行数
        
        try {
        	File file = new File("Z:\\userstatus\\afterdataclean\\needGetMaxId_f\\hayue_129");
            if(file.isDirectory()) {
                fileList = file.listFiles();
                for(File f:fileList) {
                    fis = new FileInputStream(f);
                    isr = new InputStreamReader(fis,"utf-8");
                    br = new BufferedReader(isr);
                    System.out.println(f.getName());
                    while((line = br.readLine()) != null) {
                        long startTime=System.currentTimeMillis(); 
                        ++allCount;
                        try {
                            if(line.startsWith("{")) {
                                StatusWapper wapper = Status.constructWapperStatus(IOTool.removeEol(line));
                                statusList = wapper.getStatuses();
                            }
                            if(line.startsWith("[")) {
                                statusList = Status.constructStatuses(IOTool.removeEol(line));
                            }
                            for(Status status : statusList) {
                                text = status.getText();
                                if(status.getRetweetedStatus()!=null) {
                                    rtText = status.getRetweetedStatus().getText();
                                }
                                else {
                                    rtText = "";
                                }
                                
                                //如果微博或转发微博含有事件"方舟子VS韩寒"的关键字,则输出"方舟子VS韩寒"  微博及转发微博的内容
                                for(String keyword:keywordList) {
                                    String[] list = keyword.split("\t");
                                    String find = list[1];                      
                                    Pattern p = Pattern.compile(find);
                                    Matcher matcher = p.matcher(text+rtText);
                                    if(matcher.find() && list[0].equals("方舟子VS韩寒")) {
                                        System.out.println(list[0]+"\t"+text+rtText);
                                    }
                                }
                            }
                            System.out.println(allCount);
                            long endTime=System.currentTimeMillis();		//获取结束时间
                            if((endTime-startTime)/1000>300) {
                                System.out.println("程序运行时间： "+(endTime-startTime)/1000+"s");
                            }
                        }catch (WeiboException e) {
                            e.printStackTrace();
                        } catch (weibo4j.model.WeiboException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } 
        
    }
    
    protected void readKeyword() {
        
        FileInputStream fis;
        InputStreamReader isr;
        BufferedReader br;
        String line = null;
        try {
        fis = new FileInputStream("Z:\\code\\data\\political.txt");
        isr = new InputStreamReader(fis,"utf8");
        br = new BufferedReader(isr);
        while((line = br.readLine()) != null) {
            keywordList.add(line);
        }
        } catch (IOException e) {
        	e.printStackTrace();
        }
    }
    
}