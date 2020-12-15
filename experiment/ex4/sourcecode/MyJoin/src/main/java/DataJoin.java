import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;

import com.sun.corba.se.spi.presentation.rmi.IDLNameTranslator;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.analysis.function.Exp;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DataJoin extends Configured implements Tool{

    //Mapper
    public static class DataMapper extends Mapper<LongWritable, Text, TaggedKey, Text>{

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException{

            String[] columns = value.toString().split(" ");
            TaggedKey taggedKey = new TaggedKey();

            //setting <key, value> depends on filename
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            if(fileName.startsWith("product")){
                //columns[0] is pid in product.txt, tag is 1
                taggedKey.set(columns[0], 1);
                context.write(taggedKey, value);
            } else if(fileName.startsWith("order")){
                //columns[2] is pid in order.txt, tag is 2
                taggedKey.set(columns[2], 2);
                context.write(taggedKey, value);
            }
        }

    }

    //Reducer
    public static class JoinReducer extends Reducer<TaggedKey, Text, NullWritable, Text>{

        @Override
        protected void reduce(TaggedKey key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            List<String> products = new ArrayList<String>();

            for (Text value : values) {

                switch (key.getTag().get()) {
                    case 1: // product
                        products.add(value.toString());
                        //context.write(NullWritable.get(), value);
                        break;

                    case 2: // order

                        String[] order = value.toString().split(" ");

                        for (String productString : products) {

                            String[] product = productString.split(" ");

                                List<String> output = new ArrayList<String>();
                                output.add(order[0]);
                                output.add(order[1]);
                                output.add(order[2]);
                                output.add(product[1]);
                                output.add(product[2]);
                                output.add(order[3]);
                                context.write(NullWritable.get(), new Text(StringUtils.join(output, " ")));
                        }

                        break;

                    default:
                        assert false;
                }
            }
        }
    }

    //WritableComparable
    public static class TaggedKey implements WritableComparable<TaggedKey>{

        private Text joinKey = new Text();
        private IntWritable tag = new IntWritable();

        public int compareTo(TaggedKey taggedKey){
            int compareValue = joinKey.compareTo(taggedKey.getJoinKey());
            if (compareValue == 0){
                compareValue = tag.compareTo(taggedKey.getTag());
            }
            return compareValue;
        }

        public void readFields(DataInput in) throws IOException{
            joinKey.readFields(in);
            tag.readFields(in);
        }

        public void write(DataOutput out) throws IOException{
            joinKey.write(out);
            tag.write(out);
        }

        public void set(String joinKey, int tag){
            this.joinKey.set(joinKey);
            this.tag.set(tag);
        }

        public Text getJoinKey(){
            return joinKey;
        }

        public IntWritable getTag(){
            return tag;
        }
    }

    //WritableComparator
    public static class TaggedJoinComparator extends WritableComparator{

        public  TaggedJoinComparator(){
            super(TaggedKey.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable a, WritableComparable b){
            TaggedKey key1 = (TaggedKey) a;
            TaggedKey key2 = (TaggedKey) b;
            return key1.getJoinKey().compareTo(key2.getJoinKey());
        }
    }

    //Partitioner
    public static class TaggedPartitioner extends Partitioner<TaggedKey, Text>{

        @Override
        public int getPartition(TaggedKey taggedKey, Text text, int numPartition){
            return taggedKey.getJoinKey().hashCode()%numPartition;
        }
    }

    //main
    public int run(String[] args) throws Exception{
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        FileSystem fs = FileSystem.get(getConf());
        fs.delete(outputPath, true);

        Job job = new Job(getConf());
        job.setJarByClass(DataJoin.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setMapperClass(DataMapper.class);
        job.setMapOutputKeyClass(TaggedKey.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(TaggedPartitioner.class);
        job.setGroupingComparatorClass(TaggedJoinComparator.class);

        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception{
        System.exit(ToolRunner.run(new DataJoin(), args));
    }
}

