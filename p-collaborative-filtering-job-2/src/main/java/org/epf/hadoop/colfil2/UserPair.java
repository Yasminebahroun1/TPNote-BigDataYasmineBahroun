package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class UserPair implements WritableComparable<UserPair> {
    private String user1;
    private String user2;

    // Constructeur par défaut
    public UserPair() {
        this.user1 = "";
        this.user2 = "";
    }

    // Constructeur avec paramètres
    public UserPair(String user1, String user2) {
        if (user1.compareTo(user2) < 0) {
            this.user1 = user1;
            this.user2 = user2;
        } else {
            this.user1 = user2;
            this.user2 = user1;
        }
    }

    // Getters
    public String getUser1() { return user1; }
    public String getUser2() { return user2; }

    // Sérialisation
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(user1);
        out.writeUTF(user2);
    }

    // Désérialisation
    @Override
    public void readFields(DataInput in) throws IOException {
        user1 = in.readUTF();
        user2 = in.readUTF();
    }

    @Override
    public int compareTo(UserPair other) {
        int cmp = user1.compareTo(other.user1);
        if (cmp != 0) {
            return cmp;
        }
        return user2.compareTo(other.user2);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserPair userPair = (UserPair) o;
        return Objects.equals(user1, userPair.user1) && Objects.equals(user2, userPair.user2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(user1, user2);
    }

    @Override
    public String toString() {
        return "(" + user1 + ", " + user2 + ")";
    }
}
