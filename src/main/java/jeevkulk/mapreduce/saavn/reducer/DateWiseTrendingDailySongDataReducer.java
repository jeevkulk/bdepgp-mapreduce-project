package jeevkulk.mapreduce.saavn.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DateWiseTrendingDailySongDataReducer extends Reducer<Text, Text, Text, Text> {

    /**
     * Date-wise trending songs played
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
