package jeevkulk.mapreduce.saavn.preprocess.partitioner;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PreprocessSongDataPartitioner extends Partitioner<Text, Text> implements Configurable {

    private Configuration configuration;

    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    public Configuration getConf() {
        return configuration;
    }

    /**
     * This is to partition data date-wise
     * @param songPlayedDateStr
     * @param songPlayedCountDays
     * @param i
     * @return
     */
    @Override
    public int getPartition(Text songPlayedDateStr, Text songPlayedCountDays, int i) {
        String[] fields = songPlayedDateStr.toString().split("~");
        String dateStr = fields[0];
        String dayOfMonthStr = dateStr.split("-")[2];
        return Integer.parseInt(dayOfMonthStr);
    }
}
