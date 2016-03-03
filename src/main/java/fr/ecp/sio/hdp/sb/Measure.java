package fr.ecp.sio.hdp.sb;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by charpi on 03/03/16.
 */
public class Measure implements Writable {

    public final Text heure = new Text();
    public final Text measure = new Text();

    // Do not delete
    public Measure(){
    }

    public Measure(String heure, String measure){
        this.heure.set(heure);
        this.measure.set(measure);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        heure.write(dataOutput);
        measure.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        heure.readFields(dataInput);
        measure.readFields(dataInput);

    }
}
