package fr.ecp.sio.hdp.sb;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;


/**
 * Created by charpi on 26/02/16.
 */
public class MyMapper2 extends Mapper<LongWritable, Text, CompositeKey, Measure> {

    private final static Logger LOGGER = LoggerFactory.getLogger(MyMapper.class);

    private final static Splitter SPLITTER = Splitter.on(";").omitEmptyStrings().trimResults();
    private final Text key = new Text();
    private final FloatWritable value = new FloatWritable();


    @Override
    protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {

        final List<String> tokens = Lists.newArrayList(SPLITTER.split(line.toString()));
        final String siteId = tokens.get(0);
        final String heure = tokens.get(1);
        final String measure = tokens.get(2);

        context.write(new CompositeKey(heure,siteId), new Measure(heure,measure));

    }
}
