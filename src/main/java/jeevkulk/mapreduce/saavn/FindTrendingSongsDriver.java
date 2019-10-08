package jeevkulk.mapreduce.saavn;

import jeevkulk.mapreduce.saavn.combiner.DailySongDataCombiner;
import jeevkulk.mapreduce.saavn.combiner.HourlySongDataCombiner;
import jeevkulk.mapreduce.saavn.mapper.DailySongDataMapper;
import jeevkulk.mapreduce.saavn.mapper.DateWiseTrendingSongDataMapper;
import jeevkulk.mapreduce.saavn.mapper.HourlySongDataMapper;
import jeevkulk.mapreduce.saavn.mapper.TrendingSongDataMapper;
import jeevkulk.mapreduce.saavn.reducer.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class FindTrendingSongsDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int returnStatus = ToolRunner.run(new Configuration(), new FindTrendingSongsDriver(), args);
        System.exit(returnStatus);
    }

    public int run(String[] args) throws IOException {
        int success = 1;

        String outputDir = args[1];
        getConf().set("mapreduce.app-submission.cross-platform", "true");

        /**
         * First mapreduce job to find hourly count of songs played
         */
        Job job1 = Job.getInstance(getConf());
        job1.setJobName("Saavn Hourly Trending Songs Mapper");
        job1.setJarByClass(FindTrendingSongsDriver.class);
        job1.setNumReduceTasks(24);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setMapperClass(HourlySongDataMapper.class);
        job1.setCombinerClass(HourlySongDataCombiner.class);
        job1.setReducerClass(HourlySongDataReducer.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(outputDir + "_temp1"));

        try {
            success = job1.waitForCompletion(true) ? 0 : 1;
        } catch (Exception e) {
            e.printStackTrace();
        }

        /**
         * Second mapreduce job to find top 10 hourly count of songs played
         */
        /*Job job2 = Job.getInstance(getConf());
        job2.setJobName("Saavn Hourly Trending Songs Finder");
        job2.setJarByClass(FindTrendingSongsDriver.class);
        job2.setNumReduceTasks(24);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        job2.setMapperClass(TrendingSongDataMapper.class);
        job2.setReducerClass(TrendingHourlySongDataReducer.class);

        FileInputFormat.addInputPath(job2, new Path(outputDir + "_temp1"));
        FileOutputFormat.setOutputPath(job2, new Path(outputDir + "_temp2"));

        try {
            success = job2.waitForCompletion(true) ? 0 : 1;
        } catch (Exception e) {
            e.printStackTrace();
        }*/

        /**
         * Second mapreduce job to find daily count of songs played
         */
        Job job2 = Job.getInstance(getConf());
        job2.setJobName("Saavn Daily Trending Songs Mapper");
        job2.setJarByClass(FindTrendingSongsDriver.class);
        job2.setNumReduceTasks(31);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        job2.setMapperClass(DailySongDataMapper.class);
        job2.setCombinerClass(DailySongDataCombiner.class);
        job2.setReducerClass(DailySongDataReducer.class);

        FileInputFormat.addInputPath(job2, new Path(outputDir + "_temp2"));
        FileOutputFormat.setOutputPath(job2, new Path(outputDir + "_temp3"));

        try {
            success = job2.waitForCompletion(true) ? 0 : 1;
        } catch (Exception e) {
            e.printStackTrace();
        }

        /**
         * Fourth mapreduce job to find top 100 daily count of songs played
         */
        /*Job job4 = Job.getInstance(getConf());
        job4.setJobName("Saavn Daily Trending Songs Finder");
        job4.setJarByClass(FindTrendingSongsDriver.class);
        job4.setNumReduceTasks(31);

        job4.setMapOutputKeyClass(IntWritable.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);

        job4.setMapperClass(TrendingSongDataMapper.class);
        //job4.setPartitionerClass(SongDataPartitioner.class);
        job4.setReducerClass(TrendingDailySongDataReducer.class);

        FileInputFormat.addInputPath(job4, new Path(outputDir + "_temp3"));
        FileOutputFormat.setOutputPath(job4, new Path(outputDir + "_temp4"));

        try {
            success = job4.waitForCompletion(true) ? 0 : 1;
        } catch (Exception e) {
            e.printStackTrace();
        }*/

        /**
         * Third mapreduce job to get date-wise final output
         */
        Job job3 = Job.getInstance(getConf());
        job3.setJobName("Saavn Daily Trending Songs Finder (Final)");
        job3.setJarByClass(FindTrendingSongsDriver.class);
        job3.setNumReduceTasks(31);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        job3.setMapperClass(DateWiseTrendingSongDataMapper.class);
        job3.setReducerClass(DateWiseTrendingDailySongDataReducer.class);

        FileInputFormat.addInputPath(job3, new Path(outputDir + "_temp4"));
        FileOutputFormat.setOutputPath(job3, new Path(outputDir));

        try {
            success = job3.waitForCompletion(true) ? 0 : 1;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return success;
    }
}
