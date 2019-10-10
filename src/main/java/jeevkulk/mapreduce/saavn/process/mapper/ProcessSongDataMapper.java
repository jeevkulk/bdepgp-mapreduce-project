package jeevkulk.mapreduce.saavn.process.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ProcessSongDataMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Logger logger = LoggerFactory.getLogger(ProcessSongDataMapper.class);

    /**
     * This is to map the songs grouped by Date~Song Id:
     * Job2 Input Key     : songPlayedDate~songId
     * Job2 Input Value   : songCount
     * Job2 Output Key    : songId
     * Job2 Output Value  : songCount
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int songPlayedCount = 0;
        String valueStr = value.toString();
        String[] fields = valueStr.split("\t");
        String[] songPlayedDateId = fields[0].split("~");

        String currentDate = "2017-12-25";
        if (currentDate.compareTo(songPlayedDateId[0]) > 0) {
            songPlayedCount = -1 * Integer.parseInt(fields[0]);
        } else if (currentDate.compareTo(songPlayedDateId[0]) == 0) {
            songPlayedCount = Integer.parseInt(fields[0]);
        }
        if (currentDate.compareTo(songPlayedDateId[0]) >= 0)
            context.write(new Text(songPlayedDateId[1]), new IntWritable(songPlayedCount));
    }
}
