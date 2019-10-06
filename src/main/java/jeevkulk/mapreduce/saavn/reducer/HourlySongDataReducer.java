package jeevkulk.mapreduce.saavn.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

public class HourlySongDataReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private TreeMap<Integer, String> trendingSongsMap = null;

    /*@Override
    protected void setup(Context context) throws IOException, InterruptedException {
        trendingSongsMap = new TreeMap<Integer, String>();
    }*/

    @Override
    protected void reduce(Text playedDateTimeSongStr, Iterable<IntWritable> songPlayedCountItr, Context context) throws IOException, InterruptedException {
        int count = 0;
        trendingSongsMap = new TreeMap<Integer, String>();
        for (IntWritable songPlayedCount : songPlayedCountItr) {
            count = count + songPlayedCount.get();
        }
        /*trendingSongsMap.put(count, playedDateTimeSongStr.toString());
        if (trendingSongsMap.size() > 10) {
            trendingSongsMap.remove(trendingSongsMap.firstKey());
        }*/
        context.write(new Text(playedDateTimeSongStr), new IntWritable(count));
    }

    /*@Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Integer, String> trendingSong : trendingSongsMap.entrySet()) {
            int count = trendingSong.getKey();
            String playedDateTimeSongStr = trendingSong.getValue();
            context.write(new Text(playedDateTimeSongStr), new IntWritable(count));
        }
    }*/
}
