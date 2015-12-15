package HotTopic;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * 抽取用户UID
 */
public class ExtractUidInfo {
    public static class Map extends MapReduceBase implements Mapper<LongWritable,Text, Text, Text> {
        private Text x = new Text();
        private Text y = new Text();

        private static String separator = "|#|";
        
        
        
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
           if(value.toString().contains("\t"))
           {
               String temp = value.toString().substring(value.toString().indexOf("\t")+1);
               String[] list = temp.split("\\|\\#\\|");
               if(list.length >= 6)
               {
                   x.set(list[0]);
                   y.set("1");
                   output.collect(x, y);
               }
           }
      
        }
    }
    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> 
    {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
        {
            while (values.hasNext()) 
            {
                output.collect(key, new Text(""));
                break;
            }
        }
    }
    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(ExtractUidInfo.class);
        conf.setJobName("Extract Uid");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setNumReduceTasks(1);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        //FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        Path output = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(output)){
          fs.delete(output,true);
        }
        FileOutputFormat.setOutputPath(conf, output);
        JobClient.runJob(conf);
      }
}
