package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Relationship implements Writable {
    private String id1;
    private String id2;

    // Constructeur par défaut
    public Relationship() {
        this.id1 = "";
        this.id2 = "";
    }

    // Constructeur avec paramètres
    public Relationship(String id1, String id2) {
        this.id1 = id1;
        this.id2 = id2;
    }

    // Getters et Setters
    public String getId1() { return id1; }
    public void setId1(String id1) { this.id1 = id1; }
    public String getId2() { return id2; }
    public void setId2(String id2) { this.id2 = id2; }

    // Sérialisation des données
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id1);
        out.writeUTF(id2);
    }

    // Désérialisation des données
    @Override
    public void readFields(DataInput in) throws IOException {
        id1 = in.readUTF();
        id2 = in.readUTF();
    }

    @Override
    public String toString() {
        return id1 + "<->" + id2;
    }
}
