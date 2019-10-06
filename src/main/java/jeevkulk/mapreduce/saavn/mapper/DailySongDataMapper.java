package jeevkulk.mapreduce.saavn.mapper;

import jeevkulk.mapreduce.saavn.domain.Song;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DailySongDataMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Logger logger = LoggerFactory.getLogger(DailySongDataMapper.class);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        String[] playedDateSongIdStr = fields[0].split("~");
        context.write(new Text(playedDateSongIdStr[0] + "~" + playedDateSongIdStr[2]), new IntWritable(Integer.parseInt(fields[1])));
    }
}
