package jeevkulk.mapreduce.saavn.preprocess.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class PreprocessSongDataMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Logger logger = LoggerFactory.getLogger(PreprocessSongDataMapper.class);

    /**
     * This is to map the songs grouping by Date~Song Id:
     * Job1 Input Key     : Long
     * Job1 Input Value   : songId,userId,songPlayedTimestamp,songPlayedHour,songPlayedDate
     * Job1 Output Key    : songPlayedDate~songId
     * Job1 Output Value  : songCount
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String valueStr = value.toString();
        String[] fields = valueStr.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        if (fields.length != 5) {
            logger.info("Omitting: "+valueStr);
        }
        //We are interested just in the history - later on this will be taken care by daily program
        //TODO - uncomment this line and delete below context write
        context.write(new Text(fields[4] + "~" + fields[0]), new IntWritable(1));
        /*if ("2017-12-24".compareTo(fields[4]) > 0) {
            context.write(new Text(fields[4] + "~" + fields[0]), new IntWritable(1));
        } else {
            //Do nothing
        }*/
    }
}
