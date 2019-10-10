package jeevkulk.mapreduce.saavn.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TrendingDailySongDataReducer extends Reducer<IntWritable, Text, Text, Text> {

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
     * Selected top hundred songs played on daily basis
     * Job4 Output Key      : Date~Song Id
     * Job4 Output Value    : Song Played Count~Rank
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
            if (rank < 100) {
                context.write(new Text(playedDateSongIdStr), new Text( songPlayedCount + "~" + rank));
                rank++;
            }
        }
    }
}
