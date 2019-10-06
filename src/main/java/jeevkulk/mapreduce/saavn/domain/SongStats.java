package jeevkulk.mapreduce.saavn.domain;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class SongStats implements WritableComparable<SongStats> {
    private String songId;
    private int count;

    public SongStats() {
        super();
    }

    public SongStats(String songId, int count) {
        this.songId = songId;
        this.count = count;
    }

    public String getSongId() {
        return songId;
    }

    public void setSongId(String songId) {
        this.songId = songId;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public void readFields(DataInput dataInput) throws IOException {
        songId = WritableUtils.readString(dataInput);
        count = WritableUtils.readVInt(dataInput);
    }

    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, songId);
        WritableUtils.writeVInt(dataOutput, count);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SongStats songStats = (SongStats) o;
        return songId.equals(songStats.songId);
    }

    @Override
    public int hashCode() {
        return songId.hashCode();
    }

    @Override
    public int compareTo(SongStats songStats) {
        return this.getCount() > songStats.getCount() ? 1
                : this.getCount() < songStats.getCount() ? -1
                : 0;
    }

    @Override
    public String toString() {
        return "SongStats{" +
                "songId='" + songId + '\'' +
                ", count=" + count +
                '}';
    }
}
