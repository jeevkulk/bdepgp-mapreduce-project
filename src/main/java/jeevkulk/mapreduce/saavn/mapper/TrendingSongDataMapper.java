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

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        int count = -1 * Integer.parseInt(fields[1]);
        context.write(new IntWritable(count), new Text(fields[0]));
    }
}
