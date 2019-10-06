package jeevkulk.mapreduce.saavn.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TrendingSongDataCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    protected void reduce(IntWritable songPlayedCount, Iterable<Text> playedDateTimeSongStrItr, Context context) throws IOException, InterruptedException {
        for (Text playedDateTimeSongStr : playedDateTimeSongStrItr) {
            context.write(songPlayedCount, playedDateTimeSongStr);
        }
    }
}
