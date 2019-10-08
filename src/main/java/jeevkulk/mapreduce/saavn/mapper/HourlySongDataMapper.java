package jeevkulk.mapreduce.saavn.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HourlySongDataMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Logger logger = LoggerFactory.getLogger(HourlySongDataMapper.class);

    /**
     * Maps data as per below:
     * Job1 Input Key       : Long
     * Job1 Input Value     : songId,userId,songPlayedTimestamp,songPlayedHour,songPlayedDate
     * Job1 Output Key      : songPlayedDate~songPlayedHour
     * Job1 Output Value    : songId~songCount defaulted to 1
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
        if (!fields[3].matches("[0-9]+")) {
            if ("Foxtrots, Rumbas and Quicksteps,464509e0b15187b14640d24295a463e8,1514177261,04,2017-12-24".equals(valueStr)) {
                valueStr = "Foxtrots Rumbas and Quicksteps,464509e0b15187b14640d24295a463e8,1514177261,04,2017-12-24";
                fields = valueStr.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            }
        }
        context.write(new Text(fields[4] + "~" + fields[3]), new Text(fields[0] + "~" + 1));
    }
}
