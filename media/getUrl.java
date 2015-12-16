package media;

import org.junit.Test;
import weibo4j.model.Status;
import weibo4j.model.StatusWapper;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * 分别计算微博作者信息含有地理位置，微博内容含有图片、URL的数量和比例
 * Created by dingcheng on 2015/2/5.
 */
public class getUrl {

    static int number_weibo = 0;//微博数
    static int number_geo = 0;//地址数
    static int number_url = 0;//url数
    static int number_pic = 0;//图片数

    //通过连接，下载图片
    public static void downloadPic(String imageUrl, String output, String type) throws IOException {
        String dir = output + File.separator + type;
        File f = new File(dir);
        if (!f.exists()) {
            f.mkdirs();
        }

        if (imageUrl.startsWith("http")) {
            URL url = new URL(imageUrl);
            int count = 0;
            DataInputStream dis = null;

            while (count <= 3) {
                try {
                    dis = new DataInputStream(url.openStream());
                    break;
                } catch (Exception e) {
                    count++;
                    continue;
                }
            }
            if (dis == null) {
                return;
            }
            imageUrl = imageUrl.substring(0, imageUrl.lastIndexOf(".jpg") + 4);
            imageUrl = imageUrl.substring(imageUrl.lastIndexOf("/") + 1);
            //System.out.println(imageUrl);
            String newImageName = dir + File.separator + imageUrl;

            try {
                FileOutputStream fos = new FileOutputStream(new File(newImageName));

                byte[] buffer = new byte[1024];

                int length;

                while ((length = dis.read(buffer)) > 0) {

                    fos.write(buffer, 0, length);

                }

                dis.close();

                fos.close();
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }
        }
    }

    /**
     * 从微博文件读取，解析每一条微博
     * @param path
     */
    public static void dealOneFile(String path) {
        StatusWapper wapper = null;
        String link1 = "http:\\/\\/[^\\s]*";
//                    String link2 = "http://t.cn/\\w*";
//                    String link3 = "http://sinaurl.cn/\\w*";
//                    String link4 = "http://t.cn/\\w*";
        String topic = "#[\\S\\s]*?#";
        Pattern p1 = Pattern.compile(link1);

        List<Status> statusList = new ArrayList<Status>();
        ArrayList<String> resultList = new ArrayList<String>();
        /**
         * 得到每条微博
         */
        List<String> weibo_status = Util.IOTool.readFile(path, "GBK");

        for (String line : weibo_status) {
            //文件解析错误则跳过
            if (!line.endsWith("}") || !line.startsWith("{")) {
                continue;
            }
            resultList.add(line);
            try {
                wapper = Status.constructWapperStatus(line);
                statusList = wapper.getStatuses();

                /**
                 * 每一条记录（微博）
                 */
                for (int i = 0; i < statusList.size(); i++) {
                    /**
                     * 这条微博
                     */
                    Status temp_status = statusList.get(i);
                    number_weibo++;

                    /**
                     * 地理位置
                     */
                    if (temp_status.getGeo() != null && !temp_status.getGeo().equals("")) {
                        number_geo++;
                    }
                    /**
                     * 图片信息
                     */

                    String url_pic = temp_status.getThumbnailPic();
                    if (url_pic != null && !url_pic.equals("")) {
                        number_pic++;
//                        downloadPic(url_pic,"D:\\data\\weibo\\data_example\\pic" ,"ThumbnailPic");
//                        downloadPic(temp_status.getBmiddlePic(),"D:\\data\\weibo\\data_example\\pic" ,"BmiddlePic");
//                        downloadPic(temp_status.getOriginalPic(),"D:\\data\\weibo\\data_example\\pic" ,"OriginalPic");
                    }

//                    System.out.println(temp_status.getThumbnailPic() + "|||" + temp_status.getBmiddlePic() + "|||" + temp_status.getOriginalPic());


                    /**
                     * 文本中url
                     */
                    String text = temp_status.getText();
                    if (temp_status.getText().contains("http://")) {
                        number_url++;
//                        System.out.println(temp_status.getText());
                    }
//                    Matcher matcher1 = p1.matcher(temp_status.getText());
//                    if (matcher1.find()) {
//                        String temp_url = matcher1.group(0);
//                        System.out.println(temp_url);
//                    }

                }
                statusList.clear();

            } catch (NumberFormatException e1) {
                // System.out.println(fileLevel2.getAbsolutePath());
                continue;

            } catch (Exception e) {
                System.out.println("^^^^^^^^^^^^"+e);
            }
        }
    }

    /*
    读取微博，下载其中的图片
     */
    public static void main(String args[]) {
        String dir = args[0];
        File inputDir = new File(dir);
        for (File f : inputDir.listFiles()) {
            dealOneFile(f.getAbsolutePath());
            double picper = (double) number_pic / (double) number_weibo;
            double geoper = (double) number_geo / (double) number_weibo;
            double urlper = (double) number_url / (double) number_weibo;
            System.out.println("total:"+number_weibo+"  pic:"+number_pic+"_"+picper+"  geo:"+number_geo+"_"+geoper+"  url:"+number_url+"_"+urlper);
        }
    }

    @Test
    public void testOne() {
        String path = "D:\\data\\weibo\\data_example\\weibo\\11023_3489457895976029_1585_status.txt";
        dealOneFile(path);
    }

    @Test
    public void downloadOneUrl() throws IOException {
        String url = "http://t.cn/zlulEom";
//        downloadUrl(url, "D:\\data\\weibo\\data_example\\url" + File.separator + "url1");
        String res = DownloadURL.downURL(url, "gbk", "IE8.0");
        System.out.println(res);
    }
}
