package bstatus;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

import weibo4j.model.Status;
import weibo4j.model.WeiboException;
import weibo4j.org.json.JSONArray;
import weibo4j.org.json.JSONException;

public class BScreenStatusAndRetweet {
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		String mid = null;
		String rmid = null;
		String text = null;
		String comment = null;
		String repost = null;
		String attitude = null;
		String picture = null;
		String creatAtTime = null;
		String screenName = null;
		Set<String> keywordList = new HashSet<String>();

		public void configure(JobConf job) {
			FileInputStream fis;
			InputStreamReader isr;
			BufferedReader br;
			String line = null;
			try {
				Path[] file = DistributedCache.getLocalCacheFiles(job);
				fis = new FileInputStream(file[0].toString());
				isr = new InputStreamReader(fis, "utf8");
				br = new BufferedReader(isr);
				while ((line = br.readLine()) != null) {
					keywordList.add(line);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			List<Status> statusList = null;
			Pattern pattern = Pattern.compile("(?<=\\{\"statuses\":).*(?=\\})");
			Matcher matcher = pattern.matcher(line);
			if (matcher.find()) {
				line = matcher.group();

				try {

					statusList = constructStatuses(line);

					if (statusList != null && statusList.get(0) != null
							&& statusList.get(0).getUser() != null){

						if (keywordList.contains(statusList.get(0).getUser()
								.getId())) {
							for (int i = 0; i < statusList.size(); i++) {
								try {
									mid = statusList.get(i).getMid();
									text = statusList.get(i).getText();
									comment = String.valueOf(statusList.get(i)
											.getCommentsCount());
									repost = String.valueOf(statusList.get(i)
											.getRepostsCount());
									attitude = String.valueOf(statusList.get(i)
											.getAttitudesCount());
									if (!statusList.get(i).getOriginalPic()
											.equals(""))
										picture = "1\\|#|/"
												+ statusList.get(i)
														.getOriginalPic();
									else
										picture = "0";
									output.collect(new Text("statusList.get(0).getUser().getId()"),
											new Text(mid + "\\|#|/" + text
													+ "\\|#|/" + comment
													+ "\\|#|/" + repost
													+ "\\|#|/" + attitude
													+ "\\|#|/" + picture));
								} catch (Exception e) {
									e.printStackTrace();
									continue;
								}
							}
						}
					}

					else {
							for (int i = 0; i < statusList.size(); i++) {
								try {
								if (statusList.get(i).getRetweetedStatus() != null
										&& statusList.get(i).getRetweetedStatus()
												.getUser() != null){
									if (keywordList.contains(statusList.get(i).getRetweetedStatus().getUser().getId())){
									rmid = statusList.get(i)
											.getRetweetedStatus().getMid();
									mid = statusList.get(i).getMid();
									creatAtTime = statusList.get(i)
											.getCreatedAt().toString();
									text = statusList.get(i).getText();
									screenName = statusList.get(i)
											.getUser().getScreenName();
									output.collect(new Text("r"+statusList.get(i).getRetweetedStatus().getUser().getId()),
											new Text(rmid + "\\|#|/" + mid
													+ "\\|#|/"
													+ creatAtTime
													+ "\\|#|/" + screenName
													+ "\\|#|/" + text));
									}
								}
							}catch (Exception e) {
								e.printStackTrace();
								continue;
							}
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}// if end
		}// map end

		public static List<Status> constructStatuses(String res)
				throws WeiboException, JSONException {
			JSONArray list = new JSONArray(res);
			int size = list.length();
			List<Status> statuses = new ArrayList<Status>(size);
			for (int i = 0; i < size; i++) {
				statuses.add(new Status(list.getJSONObject(i)));
			}
			return statuses;
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			while (values.hasNext()) {
				output.collect(key, values.next());
			}
		}
	}

	public static void main(String[] args) throws IOException, URISyntaxException {

		JobConf conf = new JobConf(BScreenStatusAndRetweet.class);
		conf.setJobName("WangLei331-2014");
		conf.set("mapred.jar", "/home/admin/wanglei/SSR2014.jar");
		 DistributedCache.addCacheFile(new URI("/home/wanglei/srcdata/uid.txt#uid.txt"),conf);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setNumReduceTasks(84);

		FileInputFormat.setInputPaths(conf, new Path(
				"/home/dingcheng/status/T2014"));
		Path output = new Path("hdfs://blade42:9000/home/wanglei/opdata2014");
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output)) {
			fs.delete(output, true);
		}
		FileOutputFormat.setOutputPath(conf, output);
		JobClient.runJob(conf);
	}
}
