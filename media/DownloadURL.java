package media;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 给定URL下载网页信息的类，包括指定编码进行处理和自动判断编码2种情况<br>
 * 自动判断编码的函数代价较大<br>
 * 请特别注意：Google的检索结果在本程序发送的FF的head的情况下，没有编码信息。
 * 所以在爬Google的检索结果的时候请务必手动指定使用UTF-8编码
 *
 * @author BlueJade, Fandy Wang(lfwang@ir.hit.edu.cn)
 * @version 1.0
 */
public class DownloadURL {
    /**
     * 该变量保存着寻找charset的正则表达式
     */
    private static Pattern charsetPattern = Pattern.compile(
            "charset=[\"']?([^\"']*)[\"'][ ]*[/]?", Pattern.CASE_INSENSITIVE);

    /**
     * 给定内容和编码，将内容转换成编码对应的字节码
     *
     * @param content
     * @param encoding
     * @return 转换后的结果
     */
    public static String encodeContent(String content, String encoding) {
        try {
            return URLEncoder.encode(content, encoding);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 根据字节码和相应的编码，将内容转换成对应的原始文字
     *
     * @param byteCode
     * @param encoding
     * @return 转换后的结果
     */
    public static String decodeContent(String byteCode, String encoding) {
        try {
            return URLDecoder.decode(byteCode, encoding);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * @param urladdr URL Address
     * @param type    FF3.0<br>
     *                IE8.0
     * @return HTTPURLConnection
     */
    private static HttpURLConnection getHttpURLConnectionInstance(String urladdr,
                                                                  String type) {
        URL url;
        HttpURLConnection httpUrl = null;
        try {
            url = new URL(urladdr);
            httpUrl = (HttpURLConnection) url.openConnection();
            if (type.equals("FF3.0")) {
                httpUrl
                        .addRequestProperty(
                                "User-Agent",
                                "Mozilla/5.0 (Windows; U; Windows NT 6.0; zh-CN; rv:1.9.0.8) Gecko/2009032609 Firefox/3.0.8 (.NET CLR 3.5.30729)");
                httpUrl.addRequestProperty("Accept",
                        "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
                httpUrl.addRequestProperty("Accept-Language", "zh-cn,zh;q=0.5");
                httpUrl.addRequestProperty("Keep-Alive", "300");
                httpUrl.addRequestProperty("Connection", "Keep-Alive");
            } else if (type.equals("IE8.0")) {
                httpUrl
                        .addRequestProperty(
                                "User-Agent",
                                "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.5.21022; .NET CLR 3.5.30729; .NET CLR 3.0.30729)");
                httpUrl.addRequestProperty("Accept", "*/*");
                httpUrl.addRequestProperty("Accept-Language", "zh-cn");
                httpUrl.addRequestProperty("Connection", "Keep-Alive");
            }
            httpUrl.connect();
        } catch (MalformedURLException e) {
            //e.printStackTrace();
            System.err.println("Get Web Exception in connect in stage 1!");
            httpUrl = null;
        } catch (IOException e) {
            //e.printStackTrace();
            System.err.println("Get Web Exception in IO in stage1!");
            httpUrl = null;
        }
        return httpUrl;
    }

    /**
     * 给定URL以及网页的编码，爬取网页内容
     *
     * @param urladdr
     * @param encoding 网页编码：UTF-8 or GB18030(兼容GBK、GB2312)
     * @return URL对应网页内容
     * @type 模拟浏览器抓取网页，浏览器类型：IE8.0 or FF3.0
     */
    public static String downURL(String urladdr, String encoding, String type) {
        StringBuilder result = new StringBuilder();
        try {
            HttpURLConnection httpUrl = getHttpURLConnectionInstance(urladdr, type);
            BufferedReader reader = new BufferedReader(new InputStreamReader(httpUrl
                    .getInputStream(), encoding));
            String line;
            while ((line = reader.readLine()) != null) {
                result.append(line);
                result.append('\n');
            }
            reader.close();
            httpUrl.disconnect();
        } catch (MalformedURLException e) {
            System.out.println(e); // 出现异常不退出
            result.append("");
        } catch (IOException e) {
            System.out.println(e);
            result.append("");
        }
        return result.toString();
    }

    /**
     * 给定URL以及网页，爬取网页内容，如果网页有指定编码，则本程序会自动判断编码<br>
     * 若网页没有指定编码，则默认为GB18030编码（兼容GBK，GB2312）
     *
     * @param urladdr URL
     * @param type    模拟浏览器抓取网页，浏览器类型：IE8.0 or FF3.0
     * @return URL对应网页内容
     */
    public static String downURL(String urladdr, String type) {
        System.out.println(urladdr);
        ArrayList<Byte> allbyte = new ArrayList<Byte>();
        int tryCount = 2;
        try {
            HttpURLConnection httpUrl = getHttpURLConnectionInstance(urladdr, type);
            
            //尝试2次
            while(tryCount>0 && httpUrl==null){
                httpUrl = getHttpURLConnectionInstance(urladdr, type);
                tryCount -- ;
            }
            
            if(httpUrl==null)
                return "";
            
            InputStream is = httpUrl.getInputStream();
            byte temp[] = new byte[4096];
            int readNum;
            //System.out.print("stage 1:");
            long beginTime = System.currentTimeMillis();

            /*  获取web信息，以字节的方式  */
            while ((readNum = is.read(temp, 0, 4096)) != -1) {
                for (int i = 0; i < readNum; i++) {
                    allbyte.add(temp[i]);
                }
                //System.out.print(".");
                if((System.currentTimeMillis() - beginTime)>60000){
                    //allbyte.clear();
                    //System.out.println();
                    System.out.println("Crawl out of time: "+urladdr);
                    return "";
                }
            }

            is.close();
            httpUrl.disconnect();
            //System.out.println();
            //System.out.print("stage 2:");
        } catch (MalformedURLException e) {
            //e.printStackTrace();
            System.err.println("Get "+urladdr+" Exception in connect in stage 2!");
            allbyte.clear();
        } catch (IOException e) {
            //e.printStackTrace();
            System.err.println("Get "+urladdr+" Exception in IO in stage 2!");
            allbyte.clear();
        }
        byte temp1[] = new byte[allbyte.size()];
        for (int i = 0; i < allbyte.size(); i++) {
            temp1[i] = allbyte.get(i);
        }
        String result = "";
        try {
            result = new String(temp1, "GB18030");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            result = null;
        }
        String temp2 = null;
        if (result.indexOf("<body") != -1)
            temp2 = result.substring(0, result.indexOf("<body"));
        else if (result.indexOf("<BODY") != -1)
            temp2 = result.substring(0, result.indexOf("<BODY"));
        else
            temp2 = result;
        Matcher m = charsetPattern.matcher(temp2);
        String charset = null;
        if (m.find()) {
            //charset = temp2.substring(m.start(1), m.end(1)).toLowerCase();
            m.group(1).replace("\'", "");
            m.group(1).replace("\"", "");
            charset = m.group(1).toString();
            if(charset.equals("gb2312"))
                charset = "gbk";
            try {
                result = new String(temp1, charset);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
        //System.out.println();
        return result;
    }

    /**
     * 给定URL以及网页，爬取网页内容，如果网页有指定编码，则本程序会自动判断编码<br>
     * 若网页没有指定编码，则默认为GB18030编码（兼容GBK，GB2312）
     * 重写的downURL函数，模拟的浏览器为Google
     *
     * @param urladdr URL
     * @return URL对应网页内容
     */
    public static String downURL(String urladdr){
        String result = "";

        return result;
    }

    public static void main(String[] args) {
        System.out.println(DownloadURL.downURL("http://digi.tech.qq.com/a/20120102/000092.htm", "IE8.0"));
    }

}
