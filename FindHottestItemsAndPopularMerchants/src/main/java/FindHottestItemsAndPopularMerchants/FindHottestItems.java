// package RepurchasePrediction;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.io.WritableComparable;

public class FindHottestItems{
    public static class measureItemsPopularityMapper
       extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);

        @Override
        public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            if(line[1].length() != 0 && !(line[1].equals("item_id")) && line[6].length() != 0 && !(line[6].equals("0") ))
                context.write(new Text(line[1]), one);
        }
    }

    public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                        Context context
                        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class IntWritableDecreasingComparator extends IntWritable.Comparator {  
        public int compare(WritableComparable a, WritableComparable b) {  
            return -super.compare(a, b);  
        }  

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {  
            return -super.compare(b1, s1, l1, b2, s2, l2);  
        }  
    }

    public static class SortReducer extends Reducer<IntWritable, Text, Text, NullWritable>{
        private Text result = new Text();
        int rank=1;

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) 
        throws IOException, InterruptedException{
            for(Text val: values){
                if(rank > 100){
                    break;
                }
                result.set(val.toString());
                String str="rank "+rank+": item_id="+result+", 添加购物⻋+购买+添加收藏夹="+key;
                rank++;
                context.write(new Text(str),NullWritable.get());
            }
        }
  }
}    