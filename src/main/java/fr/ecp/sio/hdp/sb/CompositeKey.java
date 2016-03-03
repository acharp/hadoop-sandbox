package fr.ecp.sio.hdp.sb;

import org.apache.commons.beanutils.converters.IntegerArrayConverter;
import org.apache.commons.collections.comparators.ComparatorChain;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.awt.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created by charpi on 03/03/16.
 */
public class CompositeKey implements WritableComparable<CompositeKey> {

    public final Text siteId = new Text();
    public final Text heure = new Text();

    public CompositeKey(){
    }

    public CompositeKey(String heure, String siteId) {
        this.siteId.set(siteId);
        this.heure.set(heure);
    }

    @Override
    public int compareTo(CompositeKey o) {
        final int level1 = this.siteId.compareTo(o.siteId);
        if (level1 != 0){
            return level1;
        }
        return o.heure.compareTo(this.heure);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.write(siteId.getBytes().length);
        siteId.write(dataOutput);
        heure.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        dataInput.readInt();
        siteId.readFields(dataInput);
        heure.readFields(dataInput);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    public static class Comparator extends WritableComparator{

        public Comparator(){
            super(CompositeKey.class);
        }

        @Override
        public int compare(byte[] o1Serialise, int offset1, int longueur1, byte[] o2Serialise, int offset2, int longueur2) {

            ByteBuffer.wrap(Arrays.copyOf(o1Serialise,4)).getInt();

            return 0;
        }
    }
}
