package jeevkulk.mapreduce.saavn.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DailySongDataReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text playedDateSongIdStr, Iterable<IntWritable> songPlayedCountItr, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable songPlayedCount : songPlayedCountItr) {
            count = count + songPlayedCount.get();
        }
        context.write(new Text(playedDateSongIdStr), new IntWritable(count));
    }
}
