package jeevkulk.mapreduce.saavn.preprocess;

import jeevkulk.mapreduce.saavn.preprocess.combiner.PreprocessSongDataCombiner;
import jeevkulk.mapreduce.saavn.preprocess.mapper.PreprocessSongDataMapper;
import jeevkulk.mapreduce.saavn.preprocess.partitioner.PreprocessSongDataPartitioner;
import jeevkulk.mapreduce.saavn.preprocess.reducer.PreprocessSongDataReducer;
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
 * This is to find historic averages for each song played in last month - we just aggregate the song
 * played count and maintain number of days span.
 * For our calculation we consider the average time a song is played from 1st Dec-2017 to previous day.
 *
 * Expected program arguments:
 * 1. Input data directory
 * 2. Output data directory where historic pre-processed data will be persisted
 *
 * Schedule:
 * This program needs to be executed only once on go-live date.
 * Daily program is designed to add that days data to this data store.
 */
public class ProcessHistoricSongsDataDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int returnStatus = ToolRunner.run(new Configuration(), new ProcessHistoricSongsDataDriver(), args);
        System.exit(returnStatus);
    }

    public int run(String[] args) throws IOException {
        int success = 1;
        String outputDir = args[1];
        getConf().set("mapreduce.app-submission.cross-platform", "true");

        /**
         * Mapreduce job to find hourly count of songs played
         */
        Job job1 = Job.getInstance(getConf());
        job1.setJobName("Saavn pre-processor of historic data");
        job1.setJarByClass(ProcessHistoricSongsDataDriver.class);
        //TODO - change this to 23
        job1.setNumReduceTasks(31);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

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
        }
        return success;
    }
}
