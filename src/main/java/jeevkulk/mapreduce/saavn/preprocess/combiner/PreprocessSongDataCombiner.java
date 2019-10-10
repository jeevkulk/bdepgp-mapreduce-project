package jeevkulk.mapreduce.saavn.preprocess.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PreprocessSongDataCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * This method aggregates the songCount of each songId
     * Job1 Input Key      : songPlayedDate~songId
     * Job1 Input Value    : songCount
     * Job1 Output Key     : songPlayedDate~songId
     * Job1 Output Value   : songCount
     * @param songPlayedDateIdStr
     * @param songPlayedCountItr
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text songPlayedDateIdStr, Iterable<IntWritable> songPlayedCountItr, Context context) throws IOException, InterruptedException {
        int songPlayedCount = 0;
        for (IntWritable songPlayedCountIntWritable : songPlayedCountItr) {
            songPlayedCount = songPlayedCount + songPlayedCountIntWritable.get();
        }
        context.write(songPlayedDateIdStr, new IntWritable(songPlayedCount));
    }
}
