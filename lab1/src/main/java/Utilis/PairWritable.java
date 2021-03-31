package Utilis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairWritable implements Writable {

    private String side;  //lado
    private String value;

    public PairWritable(String side, String value){
        set(side, value);
    }

    public void set(String side, String value){
        this.side = side;
        this.value = value;
    }

    public String getSide(){
        return this.side;
    }

    public String getValue(){
        return this.value;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeChars(side);
        out.writeChars(value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        side = in.readLine();
        value = in.readLine();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof PairWritable)) {
            return false;
        }
        PairWritable other = (PairWritable) obj;
        if (!this.side.equals(other.getSide())){
            return false;
        }
        if (!this.value.equals(other.getValue())) {
            return false;
        }
        return true;
    }
    @Override
    public String toString() {
        return this.side + "," + this.value;
    }
}
