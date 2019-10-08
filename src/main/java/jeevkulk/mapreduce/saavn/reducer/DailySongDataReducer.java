package jeevkulk.mapreduce.saavn.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class DailySongDataReducer extends Reducer<Text, Text, Text, IntWritable> {

    /**
     * Aggregates the count to get counts for "Date~Song Id" group
     * Job2 Input Key    : songPlayedDate
     * Job2 Input Value  : songId~songCount
     * Job2 Output Key   : songPlayedDate~songId
     * Job2 Output Value : songCount
     * @param playedDateStr
     * @param playedSongIdCountItr
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text playedDateStr, Iterable<Text> playedSongIdCountItr, Context context) throws IOException, InterruptedException {
        TreeMap<Integer, String> playedCountSongIdMap = new TreeMap<Integer, String>();
        String[] fields = null;
        for (Text playedSongIdCount : playedSongIdCountItr) {
            fields = playedSongIdCount.toString().split("~");
            playedCountSongIdMap.put(Integer.parseInt(fields[1]), fields[0]);
        }
        for (int i = 0; i < playedCountSongIdMap.size() && i < 10; i++) {
            Map.Entry<Integer, String> playedCountSongIdEntry = playedCountSongIdMap.lastEntry();
            if (playedCountSongIdEntry != null) {
                context.write(new Text(playedDateStr.toString() + "~" + playedCountSongIdEntry.getValue()), new IntWritable(playedCountSongIdEntry.getKey()));
            }
        }
    }
}
