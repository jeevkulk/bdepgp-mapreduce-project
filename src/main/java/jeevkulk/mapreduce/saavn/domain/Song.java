package jeevkulk.mapreduce.saavn.domain;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Song implements WritableComparable<Song> {

    private String songId;
    private String userId;
    private long songPlayedUnixTimestamp;
    private int songPlayedHour;
    private String songPlayedDateStr;

    public String getSongId() {
        return songId;
    }

    public void setSongId(String songId) {
        this.songId = songId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public long getSongPlayedUnixTimestamp() {
        return songPlayedUnixTimestamp;
    }

    public void setSongPlayedUnixTimestamp(long songPlayedUnixTimestamp) {
        this.songPlayedUnixTimestamp = songPlayedUnixTimestamp;
    }

    public int getSongPlayedHour() {
        return songPlayedHour;
    }

    public void setSongPlayedHour(int songPlayedHour) {
        this.songPlayedHour = songPlayedHour;
    }

    public String getSongPlayedDateStr() {
        return songPlayedDateStr;
    }

    public void setSongPlayedDateStr(String songPlayedDateStr) {
        this.songPlayedDateStr = songPlayedDateStr;
    }

    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, songId);
        WritableUtils.writeString(dataOutput, userId);
        WritableUtils.writeVLong(dataOutput, songPlayedUnixTimestamp);
        WritableUtils.writeVInt(dataOutput, songPlayedHour);
        WritableUtils.writeString(dataOutput, songPlayedDateStr);
    }

    public void readFields(DataInput dataInput) throws IOException {
        songId = WritableUtils.readString(dataInput);
        userId = WritableUtils.readString(dataInput);
        songPlayedUnixTimestamp = WritableUtils.readVLong(dataInput);
        songPlayedHour = WritableUtils.readVInt(dataInput);
        songPlayedDateStr = WritableUtils.readString(dataInput);
    }

    @Override
    public int compareTo(Song song) {
        return song.getSongPlayedUnixTimestamp() > this.songPlayedUnixTimestamp ? 1
                : song.getSongPlayedUnixTimestamp() < this.songPlayedUnixTimestamp ? -1
                : 0;
    }
}
