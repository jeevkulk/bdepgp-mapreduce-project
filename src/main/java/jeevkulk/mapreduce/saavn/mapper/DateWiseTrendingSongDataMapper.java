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

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        String[] playedDateSongIdStr = fields[0].split("~");
        context.write(new Text(playedDateSongIdStr[0]), new Text(playedDateSongIdStr[1] + "~" + Integer.parseInt(fields[1])));
    }
}
