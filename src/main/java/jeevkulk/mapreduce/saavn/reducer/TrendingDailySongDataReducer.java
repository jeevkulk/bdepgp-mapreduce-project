package jeevkulk.mapreduce.saavn.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TrendingDailySongDataReducer extends Reducer<IntWritable, Text, Text, Text> {

    private int count = 0;

    /**
     * Initialized count which is used to select top 10 songs
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        count = 0;
    }

    /**
     * Selected top ten songs played on an hourly basis
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
            if (count < 100) {
                context.write(new Text(playedDateSongIdStr), new Text( songPlayedCount + "~" + count));
                count ++;
            }
        }
    }
}
