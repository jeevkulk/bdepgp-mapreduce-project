package jeevkulk.mapreduce.saavn.mapper;

import jeevkulk.mapreduce.saavn.domain.Song;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HourlySongDataMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Logger logger = LoggerFactory.getLogger(HourlySongDataMapper.class);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

        Song song = new Song();
        song.setSongId(fields[0]);
        song.setUserId(fields[1]);
        if (fields[2] != null)
            song.setSongPlayedUnixTimestamp(Long.parseLong(fields[2]));
        if (fields[3] != null)
            song.setSongPlayedHour(Integer.parseInt(fields[3]));
        song.setSongPlayedDateStr(fields[4]);

        context.write(new Text(song.getSongPlayedDateStr() + "~" + song.getSongPlayedHour() + "~" + song.getSongId()), new IntWritable(1));
    }
}
