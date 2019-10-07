package jeevkulk.mapreduce.saavn.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DailySongDataMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Logger logger = LoggerFactory.getLogger(DailySongDataMapper.class);

    /**
     * Maps data as per below:
     * Key      : Date~Song Id
     * Value    : count is carried forward
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
        context.write(new Text(playedDateSongIdStr[0] + "~" + playedDateSongIdStr[2]), new IntWritable(Integer.parseInt(fields[1])));
    }
}
