package jeevkulk.mapreduce.saavn.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TrendingSongDataMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private Logger logger = LoggerFactory.getLogger(TrendingSongDataMapper.class);

    /**
     * Swaps the key and value
     * Job2 Input Key      : songPlayedDate~songPlayedHour~songId
     * Job2 Input Value    : songCount
     * Job2 Output Key     : songCount in negative to get trending songs
     * Job2 Output Value   : songPlayedDate~songPlayedHour~songId
     *
     * Job4 Output Key     : Count in negative to get trending songs
     * Job4 Output Value   : Date~Song Id
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        int count = -1 * Integer.parseInt(fields[1]);
        context.write(new IntWritable(count), new Text(fields[0]));
    }
}
