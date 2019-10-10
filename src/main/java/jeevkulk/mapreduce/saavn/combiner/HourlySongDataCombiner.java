package jeevkulk.mapreduce.saavn.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class HourlySongDataCombiner extends Reducer<Text, Text, Text, IntWritable> {

    /**
     * This method aggregates the counts of each "Date~Hour~Song Id" group
     * Job1 Input Key      : songPlayedDate~songPlayedHour
     * Job1 Input Value    : songId~songCount defaulted to 1
     * Job1 Output Key     : songPlayedDate~songPlayedHour~songId
     * Job1 Output Value   : songCount
     * @param playedDateHourStr
     * @param playedSongIdCountItr
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text playedDateHourStr, Iterable<Text> playedSongIdCountItr, Context context) throws IOException, InterruptedException {
        TreeMap<Integer, String> playedCountSongIdMap = new TreeMap<Integer, String>();
        String[] fields = null;
        for (Text playedSongIdCount : playedSongIdCountItr) {
            fields = playedSongIdCount.toString().split("~");
            playedCountSongIdMap.put(Integer.parseInt(fields[1]), fields[0]);
        }
        for (int i = 0; i < playedCountSongIdMap.size() && i < 10; i++) {
            Map.Entry<Integer, String> playedCountSongIdEntry = playedCountSongIdMap.lastEntry();
            if (playedCountSongIdEntry != null) {
                context.write(new Text(playedDateHourStr.toString() + "~" + playedCountSongIdEntry.getValue()), new IntWritable(playedCountSongIdEntry.getKey()));
            }
        }
    }
}
