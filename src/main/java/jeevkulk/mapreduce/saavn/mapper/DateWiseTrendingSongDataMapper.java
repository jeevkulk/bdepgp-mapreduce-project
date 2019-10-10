package jeevkulk.mapreduce.saavn.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DateWiseTrendingSongDataMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Logger logger = LoggerFactory.getLogger(DateWiseTrendingSongDataMapper.class);

    /**
     * Job4 Input Key      : Date~Song Id
     * Job4 Input Value    : Song Played Count~Rank
     * Job4 Output Key     : Date
     * Job4 Output Value   : Song Id~Song Played Count~Rank
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        String[] playedDateSongIdStr = fields[0].split("~");
        context.write(new Text(playedDateSongIdStr[0]), new Text(playedDateSongIdStr[1] + "~" + fields[1]));
    }
}
