package jeevkulk.mapreduce.saavn.process.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TrendingSongDataReducer extends Reducer<IntWritable, Text, Text, Text> {

    int songTrendingNumberRank = 0;
    /**
     * This method aggregates the songCount of each songId
     * Job3 Input Key      : songTrendingNumber
     * Job3 Input Value    : songId
     * Job3 Output Key     : songId
     * Job3 Output Value   : songTrendingNumber
     * @param songTrendingNumberIntWritable
     * @param songIdTextItr
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(IntWritable songTrendingNumberIntWritable, Iterable<Text> songIdTextItr, Context context) throws IOException, InterruptedException {
        for (Text songIdText : songIdTextItr) {
            songTrendingNumberRank++;
            if (songTrendingNumberRank < 100) {
                context.write(songIdText, new Text(songTrendingNumberIntWritable.get() + "~" + songTrendingNumberRank));
            } else {
                break;
            }
        }
    }
}
