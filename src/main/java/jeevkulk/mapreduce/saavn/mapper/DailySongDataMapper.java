package jeevkulk.mapreduce.saavn.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DailySongDataMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Logger logger = LoggerFactory.getLogger(DailySongDataMapper.class);

    /**
     * This is to map the songs grouping by Date~Song Id:
     * Job2 Input Key     : songPlayedDate~songPlayedHour~songId
     * Job2 Input Value   : songCount
     * Job2 Output Key    : songPlayedDate
     * Job2 Output Value  : songId~songCount
     * @param key
     * @param playedSongDateHourIdCount
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(LongWritable key, Text playedSongDateHourIdCount, Context context) throws IOException, InterruptedException {
        String[] fields = playedSongDateHourIdCount.toString().split("\t");
        String[] playedSongDateIdStr = fields[0].split("~");
        context.write(new Text(playedSongDateIdStr[0]), new Text(playedSongDateIdStr[2] + "~" + fields[1]));
    }
}
