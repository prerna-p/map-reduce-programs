package kmeans;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMeansTwitter
{
    private static final Logger logger = LogManager.getLogger(KMeansTwitter.class);
    public static enum COUNTER {SSETRACKER};

    public static class CentroidMapper extends Mapper<Object, Text, FloatWritable, Text> {
        private String centers = null;
        List<Float> centerList;// = new ArrayList<>(4);

        public void setup(Context context) throws IOException {

            //centers = context.getConfiguration().get("CENTROIDS");
            Configuration config = context.getConfiguration();
            centers = config.get("CENTROIDS");
            int kval = config.getInt("KVAL",4);
            centerList = new ArrayList<>(kval);
            FileSystem fs = FileSystem.get(URI.create(centers),config);
            FileStatus[] files = fs.listStatus(new Path(centers));
            if (files == null || files.length == 0) {
                throw new RuntimeException(
                        "Centroid information is not set");
            }

            for (FileStatus f : files) {
                BufferedReader rdr = new BufferedReader(
                        new InputStreamReader(fs.open(f.getPath())));
                String line;
                while ((line = rdr.readLine()) != null) {
                    centerList.add(Float.parseFloat(line));
                }
            }

        }

        private float distance(float a, float b){
            return Math.abs(a-b);
        }

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            String[] record = value.toString().split(",");
            int user = Integer.parseInt(record[0]);
            float followers = Float.parseFloat(record[1]);
            float closestCenter  = centerList.get(0);
            float minDist = distance(closestCenter,followers);

            for(float c : centerList){
                if (distance(c,followers) < minDist){
                    closestCenter = c;
                    minDist = distance(closestCenter,followers);
                }
            }
            String outVal = record[0] + "," + record[1];
            context.write(new FloatWritable(closestCenter),value);

        }

        public void cleanup(Context context) throws InterruptedException, IOException {
            for(float c : centerList){
                context.write(new FloatWritable(c), new Text("-0"));
            }
        }
    }

    public static class CentroidReducer extends Reducer<FloatWritable, Text, FloatWritable, NullWritable> {
        List<Float> errList = new ArrayList<>(4);

        @Override
        public void reduce(final FloatWritable key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {

            float updatedCenter = 0;
            int counts = 0;
            float sse1 = 0f;
            float M = Float.parseFloat(key.toString());
            List<Float> valCopy = new ArrayList<>();

            for(Text tuple : values){
                if(!tuple.toString().equals("-0")){
                    String followers = tuple.toString().split(",")[1];
                    updatedCenter+=Float.parseFloat(followers);
                    //sse1 += errors(M,Float.parseFloat(followers));
                    counts++;
                    valCopy.add(Float.parseFloat(followers));
                }
            }

            // SET UPDATED CENTROIDS
            if(updatedCenter == 0) {
                updatedCenter = M;
            }
            else{
                updatedCenter = updatedCenter/counts;
                //CALCULATE SSE
                for(float followerCount : valCopy){
                    sse1 += errors(updatedCenter,followerCount);
                }
            }

            errList.add(sse1);

            context.write(new FloatWritable(updatedCenter),NullWritable.get());
        }

        private float errors(float mi, float x) {
            float sq = (mi - x) * (mi - x);
            return sq;

        }

        public void cleanup(Context context){

            float sum = 0f;
            for(Float err : errList){
                sum = sum + err;
            }
            context.getCounter(COUNTER.SSETRACKER).setValue((long)sum);
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // set centroids file as a config
        String fnme = args[1];
        String fnme1 = "centroid-0";
        conf.set("CENTROIDS", new Path(fnme1).toString());
        conf.setInt("KVAL",Integer.parseInt(args[2]));
        Job job = Job.getInstance(conf, "K Means");
        job.setJarByClass(KMeansTwitter.class);
        job.setMapperClass(CentroidMapper.class);
        job.setMapOutputKeyClass(FloatWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(CentroidReducer.class);
        job.setOutputKeyClass(FloatWritable.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(fnme+"1"));

        job.waitForCompletion(true);
        Counter tcnt = job.getCounters().findCounter(COUNTER.SSETRACKER);
        long oldSSE = ((Counter) tcnt).getValue();
        long nextSSE;

        int i = 1;
        while(i<10){
            String outFile = fnme + Integer.toString(i);
            conf.set("CENTROIDS", new Path(outFile).toString());
            Job job2 = Job.getInstance(conf, "K Means itr");
            job2.setJarByClass(KMeansTwitter.class);
            job2.setMapperClass(CentroidMapper.class);
            job2.setMapOutputKeyClass(FloatWritable.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setReducerClass(CentroidReducer.class);
            job2.setOutputKeyClass(FloatWritable.class);
            job2.setOutputValueClass(NullWritable.class);
            FileInputFormat.addInputPath(job2, new Path(args[0]));
            i++;
            outFile = "";
            outFile = fnme + Integer.toString(i);
            FileOutputFormat.setOutputPath(job2, new Path(outFile));
            if( !job2.waitForCompletion(true)){
                logger.error("MapReduce Job failed to complete");
            }
            tcnt = job2.getCounters().findCounter(COUNTER.SSETRACKER);
            nextSSE = ((Counter) tcnt).getValue();
            if(oldSSE == nextSSE){
                logger.info("****************************** SSE MATCHES ******************************");
                break;
            }

            else{
                oldSSE = nextSSE;
                nextSSE = 0;
            }
        }



    }
}
