package jeevkulk.mapreduce.saavn.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DailySongDataCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text playedDateSongIdStr, Iterable<IntWritable> songPlayedCountItr, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable songPlayedCount : songPlayedCountItr) {
            count = count + songPlayedCount.get();
        }
        context.write(playedDateSongIdStr, new IntWritable(count));
    }
}
