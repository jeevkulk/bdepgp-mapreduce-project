package jeevkulk.mapreduce.saavn.process;

import jeevkulk.mapreduce.saavn.process.mapper.ProcessSongDataMapper;
import jeevkulk.mapreduce.saavn.process.mapper.TrendingSongDataMapper;
import jeevkulk.mapreduce.saavn.process.reducer.ProcessSongDataReducer;
import jeevkulk.mapreduce.saavn.process.reducer.TrendingSongDataReducer;
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

/**
 * To find trending song we use the following formula:
 * 1. Calculate "trendingNumber" = (Number of times a song is played the previous day)
 *              - (Average Number of times a song is played in last 30 days)
 * 2. Top 100 songs with highest "trendingNumber" calculated in step 1 are considered trending.
 *
 * What this program does:
 * This is to find "trendingNumber" for the day passed.
 * For our calculation we consider the average time a song is played from 1st Dec-2017 to previous day.
 *
 * Expected program arguments:
 * 1. Input data directory
 * 2. Output data directory where trending data is to be persisted
 * 3. Date for which the processing is to be done - this will be current date
 *
 * Schedule:
 * Job1 of this program adds last days data to the data store;
 * Job2 of this program calculates "trendingNumber";
 * Job3 of this program finds top hundred trending songs for the day and persists it.
 */
public class FindTrendingSongsDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int returnStatus = ToolRunner.run(new Configuration(), new FindTrendingSongsDriver(), args);
        System.exit(returnStatus);
    }

    public int run(String[] args) throws IOException {
        int success = 1;
        String outputDir = args[1];
        String currentDate = args[2]; //Current date in YYYY-MM-DD format
        getConf().set("mapreduce.app-submission.cross-platform", "true");
        getConf().set("currentDate", currentDate);

        /**
         * Mapreduce job to find daily count of songs played - same is added to historicData on a daily basis
         */
        //TODO: Uncomment this block
        /*Job job1 = Job.getInstance(getConf());
        job1.setJobName("Saavn processor of last days data");
        job1.setJarByClass(FindTrendingSongsDriver.class);
        job1.setNumReduceTasks(1);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setMapperClass(PreprocessSongDataMapper.class);
        job1.setCombinerClass(PreprocessSongDataCombiner.class);
        job1.setPartitionerClass(PreprocessSongDataPartitioner.class);
        job1.setReducerClass(PreprocessSongDataReducer.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path( outputDir + "_historic_data"));

        try {
            success = job1.waitForCompletion(true) ? 0 : 1;
        } catch (Exception e) {
            e.printStackTrace();
        }*/

        /**
         * Second mapreduce job to find trendingNumber for
         */
        Job job2 = Job.getInstance(getConf());
        job2.setJobName("Saavn Daily Trending Songs Mapper");
        job2.setJarByClass(FindTrendingSongsDriver.class);
        job2.setNumReduceTasks(1);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        job2.setMapperClass(ProcessSongDataMapper.class);
        job2.setReducerClass(ProcessSongDataReducer.class);

        FileInputFormat.addInputPath(job2, new Path(outputDir + "_historic_data"));
        FileOutputFormat.setOutputPath(job2, new Path(outputDir + "_unsorted_trending_data"));

        try {
            success = job2.waitForCompletion(true) ? 0 : 1;
        } catch (Exception e) {
            e.printStackTrace();
        }

        /**
         * Third mapreduce job to find trendingNumber for
         */
        Job job3 = Job.getInstance(getConf());
        job3.setJobName("Saavn Ranked Daily Trending Songs Mapper");
        job3.setJarByClass(FindTrendingSongsDriver.class);
        job3.setNumReduceTasks(1);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(IntWritable.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);

        job3.setMapperClass(TrendingSongDataMapper.class);
        job3.setReducerClass(TrendingSongDataReducer.class);

        FileInputFormat.addInputPath(job3, new Path(outputDir + "_unsorted_trending_data"));
        FileOutputFormat.setOutputPath(job3, new Path(outputDir + "_trending_data"));

        try {
            success = job3.waitForCompletion(true) ? 0 : 1;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return success;
    }
}
