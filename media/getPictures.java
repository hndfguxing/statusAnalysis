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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 下载微博内容中的图片
 * Created by dingcheng on 2015/1/29.
 */
public class getPictures {

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
            
            //尝试3次
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
                    String url_pic = temp_status.getThumbnailPic();
                    if (url_pic != null && !url_pic.equals("")) {
                        downloadPic(url_pic,"D:\\data\\weibo\\data_example\\pic" ,"ThumbnailPic");
                        downloadPic(temp_status.getBmiddlePic(),"D:\\data\\weibo\\data_example\\pic" ,"BmiddlePic");
                        downloadPic(temp_status.getOriginalPic(),"D:\\data\\weibo\\data_example\\pic" ,"OriginalPic");
                    }

                    System.out.println(temp_status.getThumbnailPic() + "|||" + temp_status.getBmiddlePic() + "|||" + temp_status.getOriginalPic());



                    String link = "http:\\/\\/[^\\s]*";
                    String topic = "#[\\S\\s]*?#";
                    Pattern p = Pattern.compile(topic);
                    Pattern p1 = Pattern.compile(link);

                    Matcher matcher = p.matcher(temp_status.getText());
                    Matcher matcher1 = p1.matcher(temp_status.getText());

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
        }
    }

    @Test
    public void testOne() {
        String path = "D:\\data\\weibo\\data_example\\weibo\\11023_3489457895976029_1585_status.txt";
        dealOneFile(path);
    }
}
