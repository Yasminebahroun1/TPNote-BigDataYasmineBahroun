package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserPairWritable implements WritableComparable<UserPairWritable> {
    private String user1;
    private String user2;

    public UserPairWritable() {
        this.user1 = "";
        this.user2 = "";
    }

    public UserPairWritable(String user1, String user2) {
        this.user1 = user1;
        this.user2 = user2;
    }

    public void set(String user1, String user2) {
        this.user1 = user1;
        this.user2 = user2;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(user1);
        out.writeUTF(user2);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        user1 = in.readUTF();
        user2 = in.readUTF();
    }

    @Override
    public int compareTo(UserPairWritable o) {
        int cmp = user1.compareTo(o.user1);
        if (cmp != 0) {
            return cmp;
        }
        return user2.compareTo(o.user2);
    }

    @Override
    public String toString() {
        return user1 + "," + user2;
    }
}
