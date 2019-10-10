package jeevkulk.mapreduce.saavn.process.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ProcessSongDataReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * This method aggregates the songCount of each songId
     * Job2 Input Key      : songId
     * Job2 Input Value    : songCount
     * Job2 Output Key     : songId
     * Job2 Output Value   : songTrendingNumber
     * @param songIdText
     * @param songCountIntWritableItr
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text songIdText, Iterable<IntWritable> songCountIntWritableItr, Context context) throws IOException, InterruptedException {
        int currentDaySongCount = 0;
        int historicSongCount = 0;
        int songPlayedCount = 0;
        String currentDate = "2017-12-25";
        for (IntWritable songCountIntWritable : songCountIntWritableItr) {
            int songCount = songCountIntWritable.get();
            if (songCount < 0) {
                historicSongCount = historicSongCount - songCount;
            } else if (songCount > 0) {
                currentDaySongCount = currentDaySongCount + songCount;
            }
        }
        int day = Integer.parseInt(currentDate.split("-")[2]);
        int trendingNumber = currentDaySongCount + historicSongCount / day;
        context.write(songIdText, new IntWritable(trendingNumber));
    }
}
