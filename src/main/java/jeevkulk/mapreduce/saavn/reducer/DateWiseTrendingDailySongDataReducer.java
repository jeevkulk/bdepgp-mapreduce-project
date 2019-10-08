package jeevkulk.mapreduce.saavn.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DateWiseTrendingDailySongDataReducer extends Reducer<Text, Text, Text, Text> {

    /**
     * Date-wise trending songs played
     * Job4 Input Key     : Date
     * Job4 Input Value   : Song Id~Song Played Count~Rank
     * Job4 Output Key    : Song Id
     * Job4 Output Value  : Date~Rank
     * @param songPlayedDateText
     * @param playedSongIdCountStrItr
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text songPlayedDateText, Iterable<Text> playedSongIdCountStrItr, Context context) throws IOException, InterruptedException {
        for (Text playedDateSongIdStr : playedSongIdCountStrItr) {
            String[] keyFields = playedDateSongIdStr.toString().split("~");
            context.write(new Text(keyFields[0]),  new Text(songPlayedDateText.toString() + "~" + keyFields[2]));
        }
    }
}
