package com.twitterfollowercount.app;

/**
 * this program emits cardinality of triangles found in Twitter dataset using Replicated Join where 
 * the input is a truncated version of the original dataset depending on the Max Filter 
 * value (executed from MaxFilter.java)
 */


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class RepJoin {

	private static final Logger logger = LogManager.getLogger(RepJoin.class);
	public static enum COUNTER {TRIANGLECNT};

	public static class FilterMapper extends Mapper<Object, Text, Text, NullWritable> {
		private Text user = new Text();
		private String MAX_VAL = null;

		public void setup(Context context){
			MAX_VAL = context.getConfiguration().get("MAX");
		}

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

			//For each record (r1,r2), split by comma
			String record = value.toString();
			String[] entry = record.split(",");

			int max_val, user1, user2;
			max_val = Integer.parseInt(MAX_VAL);
			user1 = Integer.parseInt(entry[0]);
			user2 = Integer.parseInt(entry[1]);

			// filter out users whose user-id > MAX_VAL
			if(user1 < max_val && user2 < max_val) {
				user.set(record);
				context.write(user,NullWritable.get());
			}

		}
	}

	public static class RepJoinMapper extends
			Mapper<Object, Text, NullWritable, NullWritable> {

		private HashMap<String, List<String>> fromMap = new HashMap<String, List<String>>();
		
		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			try {
				Configuration config = context.getConfiguration();
				String name = config.get("INPUT");
				FileSystem fs = FileSystem.get(URI.create(name),config);
				FileStatus[] files = fs.listStatus(new Path(name));

				if (files == null || files.length == 0) {
					throw new RuntimeException(
							"User information is not set");
				}

				for (FileStatus f : files) {
					BufferedReader rdr = new BufferedReader(
							new InputStreamReader(fs.open(f.getPath())));

					String line;
					// construct HashMap for each record in the input file
					while ((line = rdr.readLine()) != null) {
						String[] record = line.split(",");
						List<String> fromList = new ArrayList<String>();

						if(fromMap.containsKey(record[0])){
							List<String> l = fromMap.get(record[0]);

							l.add(record[1]);
							fromMap.put(record[0],l);
						}
						else{
							fromList.add(record[1]);
							fromMap.put(record[0],fromList);
						}
					}
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			Text t = new Text();
			String[] nodes = value.toString().split(",");

			/* for each input node pair, iterate through the child(ren) of the "to"
			 * node and find occurence of the to-parent
			 */
			if(fromMap.containsKey(nodes[1])){
				List<String> toNodes = fromMap.get(nodes[1]);
				if(!toNodes.isEmpty()) {
					for (String s : toNodes) {
						if(fromMap.containsKey(s)){
							List<String> sList = fromMap.get(s);
							for(String x : sList){
								if(x.equals(nodes[0])){
									context.getCounter(COUNTER.TRIANGLECNT).increment(1);
									//String tmp =nodes[0]+","+nodes[1];
									//t.set(tmp);
									//context.write(NullWritable.get(), NullWritable.get());
								}
							}
						}
					}
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {


		// MAX FILTER
		final Configuration conf1 = new Configuration();
		Job job1 = Job.getInstance(conf1, "MaxFilter");
		job1.setJarByClass(RepJoin.class);
		Configuration jobConf = job1.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");

		String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
		if (otherArgs.length != 4) {
			logger.error("Usage: ReplicatedJoin <input data> <max output> <max-filter> <output>");
			System.exit(1);
		}


		String MAX = otherArgs[2];
		// set MAX_FILTER value
		job1.setMapperClass(FilterMapper.class);
		jobConf.set("MAX", MAX);
		logger.info("MAX FILTER: "+ MAX);


		job1.setNumReduceTasks(0);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));

		if( !job1.waitForCompletion(true)){
			logger.error("MapReduce Job 1 failed to complete");
		}

		// REP JOIN
		Configuration conf = new Configuration();

		// set input file as a config
		conf.set("INPUT", new Path(otherArgs[1]).toString());
		logger.info(otherArgs[1]);

		Job job = new Job(conf, "Replicated Join");

		job.setJarByClass(RepJoin.class);


		job.setMapperClass(RepJoinMapper.class);
		job.setNumReduceTasks(0);

		TextInputFormat.setInputPaths(job, new Path(otherArgs[1]));
		TextOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);

		int retVal = job.waitForCompletion(true) ? 0 : 1;
		if(retVal==0){
			Counter tcnt = job.getCounters().findCounter(COUNTER.TRIANGLECNT);
			long c = ((Counter) tcnt).getValue();
			c/=3;
			logger.info("\n************************************************************************");
			logger.info("\n"+ c);
			logger.info("\n************************************************************************");
		}



		}

}
