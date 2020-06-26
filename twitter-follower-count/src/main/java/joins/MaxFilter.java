package joins;

/**
 * this program emits (user, followee) pairs depending on the filter value set in MAX_VALUE from command line
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

public class MaxFilter extends Configured implements Tool {
  private static final Logger logger = LogManager.getLogger(MaxFilter.class);

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

  public int run(final String[] args) throws Exception {
    final Configuration conf = getConf();
    final Job job = Job.getInstance(conf, "MaxFilter");
    job.setJarByClass(MaxFilter.class);
    final Configuration jobConf = job.getConfiguration();
    jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
    // Delete output directory, only to ease local development; will not work on
    // AWS. ===========
    /*
     * final FileSystem fileSystem = FileSystem.get(conf); if (fileSystem.exists(new
     * Path(args[1]))) { fileSystem.delete(new Path(args[1]), true); }
     */
    // ================
    String MAX = args[2];
    job.setMapperClass(FilterMapper.class);

    // set MAX_FILTER value
    jobConf.set("MAX", MAX);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(final String[] args) {
    if (args.length != 3) {
      throw new Error("Three arguments required:\n<input-dir> <output-dir> <max-value>");
    }

    try {
      ToolRunner.run(new MaxFilter(), args);
    } catch (final Exception e) {
      logger.error("", e);
    }
  }
}
