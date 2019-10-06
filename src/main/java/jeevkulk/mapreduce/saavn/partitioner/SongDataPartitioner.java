package jeevkulk.mapreduce.saavn.partitioner;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SongDataPartitioner extends Partitioner<Text, IntWritable> implements Configurable {

    private Configuration configuration;

    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    public Configuration getConf() {
        return configuration;
    }

    @Override
    public int getPartition(Text songPlayedDateStr, IntWritable songCount, int i) {
        String[] fields = songPlayedDateStr.toString().split("~");
        String dateStr = fields[0];
        String dayOfMonthStr = dateStr.split("-")[2];
        return Integer.parseInt(dayOfMonthStr);
    }
}
