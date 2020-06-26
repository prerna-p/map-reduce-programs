package joins;

package Joins;

/**
 * this program emits cardinality of triangles found in Twitter dataset using RS Join where 
 * the input is a truncated version of the original dataset depending on the Max Filter 
 * value (executed from MaxFilter.java)
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RSJoin extends Configured implements Tool {
  private static final Logger logger = LogManager.getLogger(RSJoin.class);

  public static enum COUNTER {
    TRAINGLE_CNT
  };

  public static class FilterMapper extends Mapper<Object, Text, Text, NullWritable> {
    private Text user = new Text();
    private String MAX_VAL = null;

    public void setup(Context context) {
      MAX_VAL = context.getConfiguration().get("MAX");
    }

    @Override
    public void map(final Object key, final Text value, final Context context)
        throws IOException, InterruptedException {

      // For each record (r1,r2), split by comma
      String record = value.toString();
      String[] entry = record.split(",");

      int max_val, user1, user2;
      max_val = Integer.parseInt(MAX_VAL);
      user1 = Integer.parseInt(entry[0]);
      user2 = Integer.parseInt(entry[1]);

      // filter out users whose user-id > MAX_VAL
      if (user1 < max_val && user2 < max_val) {
        user.set(record);
        context.write(user, NullWritable.get());
      }

    }
  }

  public static class FromMapper extends Mapper<Object, Text, Text, Text> {
    private final String FROM = new String("from");
    private Text fromUser = new Text();
    private Text fromValue = new Text();

    @Override
    public void map(final Object key, final Text value, final Context context)
        throws IOException, InterruptedException {

      /*
       * For each record (user,followee), split by comma and emit:
       * user,(user,followee,FROM)
       */
      String record = value.toString();
      String[] entry = record.split(",");

      String fromVal = record + "," + FROM;
      fromUser.set(entry[0]);
      fromValue.set(fromVal);
      context.write(fromUser, fromValue);

    }
  }

  public static class StepTwoMapper extends Mapper<Object, Text, Text, Text> {
    private final String TO = new String("to");
    private Text emitKey = new Text();
    private Text emitVal = new Text();

    @Override
    public void map(final Object key, final Text value, final Context context)
        throws IOException, InterruptedException {

      /*
       * For each record (start,mid,end), split by comma and emit: end,(start,end,TO)
       */
      String edge = value.toString();
      String[] nodes = edge.split(",");
      String val = nodes[0] + "," + nodes[2] + "," + TO;
      emitKey.set(nodes[2]);
      emitVal.set(val);
      context.write(emitKey, emitVal);

    }
  }

  public static class TraingleCountReducer extends Reducer<Text, Text, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      List<String> fromlist = new ArrayList<String>();
      List<String> tolist = new ArrayList<String>();
      String[] r = null;
      List<String> result = new ArrayList<String>();
      Text cnt1 = new Text();

      // separate the input values into TO & FROM lists
      for (Text val : values) {
        r = val.toString().split(",");
        if (r[2].equals("to")) {
          tolist.add(r[0] + "," + r[1]);
        } else if (r[2].equals("from")) {
          fromlist.add(r[0] + "," + r[1]);
        }

      }

      /*
       * generate all pairs for which the tuple in FROM matches tuple in TO
       */
      for (String edgeF : fromlist) {
        for (String edgeT : tolist) {
          String[] nodeF = edgeF.split(",");
          String[] nodeT = edgeT.split(",");
          if (nodeF[0].equals(nodeT[1]) && nodeF[1].equals(nodeT[0])) {
            context.getCounter(COUNTER.TRAINGLE_CNT).increment(1);
            String tmp = nodeF[0] + "," + nodeF[1];
            // result.add(tmp);
            // cnt1.set(result.toString());
            /// context.write(cnt1,NullWritable.get());

          }
        }
      }

    }
  }

  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
    private final String TO = new String("to");
    private final String FROM = new String("from");
    private Text fromUser = new Text();
    private Text fromValue = new Text();
    private Text toUser = new Text();
    private Text toValue = new Text();

    @Override
    public void map(final Object key, final Text value, final Context context)
        throws IOException, InterruptedException {

      /*
       * For each record (user,followee), split by comma and emit:
       * user,(user,followee,FROM) followee,(user,followee,TO)
       */
      String record = value.toString();
      String[] entry = record.split(",");

      String fromVal = record + "," + FROM;
      fromUser.set(entry[0]);
      fromValue.set(fromVal);
      context.write(fromUser, fromValue);

      String toVal = record + "," + TO;
      toUser.set(entry[1]);
      toValue.set(toVal);
      context.write(toUser, toValue);

    }
  }

  public static class CustomPartitioner extends Partitioner<Text, Text> {

    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
      return key.toString().hashCode();
    }
  }

  public static class ListReducer extends Reducer<Text, Text, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      List<String> fromlist = new ArrayList<String>();
      List<String> tolist = new ArrayList<String>();
      String[] r = null;
      Text l = new Text();
      // separate the input values into TO & FROM lists
      for (Text val : values) {
        r = val.toString().split(",");
        if (r[2].equals("to")) {
          tolist.add(r[0] + "," + r[1]);
        } else if (r[2].equals("from")) {
          fromlist.add(r[0] + "," + r[1]);
        }

      }

      // generate 2 length paths by matching from-list.to node to
      // to-list.from node
      for (String edgeF : fromlist) {
        for (String edgeT : tolist) {
          String[] nodeF = edgeF.split(",");
          String[] nodeT = edgeT.split(",");
          if (nodeF[1].equals(nodeT[0])) {
            l.set(nodeF[0] + "," + nodeF[1] + "," + nodeT[1]);
            context.write(l, NullWritable.get());
          }
          if (nodeF[0].equals(nodeT[1])) {
            l.set(nodeT[0] + "," + nodeT[1] + "," + nodeF[1]);
            context.write(l, NullWritable.get());
          }
        }
      }

    }
  }

  public int run(final String[] args) throws Exception {

    // MAX FILTER
    final Configuration conf1 = new Configuration();
    Job job1 = Job.getInstance(conf1, "MaxFilter");
    job1.setJarByClass(RSJoin.class);
    Configuration jobConf1 = job1.getConfiguration();
    jobConf1.set("mapreduce.output.textoutputformat.separator", "\t");

    String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
    if (otherArgs.length != 5) {
      logger.error("Usage: ReplicatedJoin <input data> <max output> <max-filter> <output>");
      System.exit(1);
    }

    String MAX = otherArgs[2];
    // set MAX_FILTER value
    job1.setMapperClass(FilterMapper.class);
    jobConf1.set("MAX", MAX);
    logger.info("MAX FILTER: " + MAX);

    job1.setNumReduceTasks(0);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(NullWritable.class);
    FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));

    if (!job1.waitForCompletion(true)) {
      logger.error("MapReduce Job 1 failed to complete");
    }

    // RS JOIN STEP-1

    final Configuration conf = getConf();
    final Job job = Job.getInstance(conf, "RS Join");
    job.setJarByClass(RSJoin.class);
    final Configuration jobConf = job.getConfiguration();
    jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
    // Delete output directory, only to ease local development; will not work on
    // AWS. ===========
    /*
     * final FileSystem fileSystem = FileSystem.get(conf); if (fileSystem.exists(new
     * Path(args[1]))) { fileSystem.delete(new Path(args[1]), true); }
     */
    // ================
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(ListReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
    if (!job.waitForCompletion(true)) {
      logger.error("MapReduce Job 1 failed to complete");
      return 0;
    }

    /*
     * `* STEP 2 JOB
     */

    final Configuration conf2 = getConf();
    final Job job2 = Job.getInstance(conf2, "RS Join1");
    job2.setJarByClass(RSJoin.class);
    final Configuration jobConf2 = job2.getConfiguration();
    jobConf2.set("mapreduce.output.textoutputformat.separator", "\t");

    MultipleInputs.addInputPath(job2, new Path(otherArgs[1]), TextInputFormat.class, FromMapper.class);

    MultipleInputs.addInputPath(job2, new Path(otherArgs[3]), TextInputFormat.class, StepTwoMapper.class);
    job2.setReducerClass(TraingleCountReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);

    FileOutputFormat.setOutputPath(job2, new Path(otherArgs[4]));
    int retVal = job2.waitForCompletion(true) ? 0 : 1;
    if (retVal == 0) {
      Counter tcnt = job2.getCounters().findCounter(COUNTER.TRAINGLE_CNT);
      long c = ((Counter) tcnt).getValue();
      c /= 3;
      logger.info("\n************************************************************************");
      logger.info("\n" + c);
      logger.info("\n************************************************************************");
    }

    return retVal;
  }

  public static void main(final String[] args) {
    if (args.length != 5) {
      throw new Error("Three arguments required:\n<input-dir> <max-output-dir> <max-val> <step1-output> <output>");
    }

    try {
      ToolRunner.run(new RSJoin(), args);
    } catch (final Exception e) {
      logger.error("", e);
    }
  }
}
