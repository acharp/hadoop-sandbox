package fr.ecp.sio.hdp.sb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by charpi on 26/02/16.
 *
 * Cette classe driver est la glue qui fait fonctionner le Mapper et le Reducer ensemble.
 * Elle gère la config, les entrées et le lancement du job MapReduce.
 */
public class MyDriver extends Configured implements Tool{

    public static void main(String[] args) throws Exception {

        final int exitCode = ToolRunner.run(new MyDriver(), args);
        System.exit(exitCode);

    }

    @Override
    public int run(String[] args) throws Exception {

        final Job job = Job.getInstance(getConf(), "sample-job");

        job.setJarByClass(MyDriver.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass((MyMapper.class));
        job.setNumReduceTasks(0);

        job.submit();
        int exitCode = job.waitForCompletion(true) ? 0 : 1;
        return exitCode;
    }
}
