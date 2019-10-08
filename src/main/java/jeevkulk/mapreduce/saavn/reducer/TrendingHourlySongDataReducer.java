package jeevkulk.mapreduce.saavn.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TrendingHourlySongDataReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

    private int rank = 0;

    /**
     * Initialized count which is used to select top 10 songs
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        rank = 0;
    }

    /**
     * Selected top ten song-counts played on an hourly basis
     * Job2 Input Key      : songCount in negative to get trending songs
     * Job2 Input Value    : songPlayedDate~songPlayedHour~songId
     * Job2 Output Key     : Date~Hour~Song Id
     * Job2 Output Value   : sum of count
     * @param songPlayedCountIntWritable
     * @param playedDateSongIdStrItr
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(IntWritable songPlayedCountIntWritable, Iterable<Text> playedDateSongIdStrItr, Context context) throws IOException, InterruptedException {
        int songPlayedCount = 0;
        for (Text playedDateSongIdStr : playedDateSongIdStrItr) {
            songPlayedCount = -1 * songPlayedCountIntWritable.get();
            if (rank < 10) {
                context.write(playedDateSongIdStr, new IntWritable(songPlayedCount));
            }
        }
        rank++;
    }
}
