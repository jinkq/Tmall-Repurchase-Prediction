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

import UserLog;

public class FindPopularMerchantsAmongYoung{
    public static class MergeTableMapper
       extends Mapper<Object, Text, Text, UserLog>{

        @Override
        public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            UserLog userLog = new UserLog();
            if(!line[0].equals("user_id"){
                String userID = line[0];
            }
            
            if(line.length==7){//user log
                if(!(line[0].equals("user_id")) && !(line[3].equals("seller_id") ) && !(line[6].equals("0") )){
                    userLog.setUserID(line[0]);
                    userLog.setSellerID(line[3]);
                    context.write(new Text(userID), userLog);
                }
            }
            else if(line.length==3){//user info
                if(!(line[0].equals("user_id")) &&  (line[1].equals("1") || line[1].equals("2") || line[1].equals("3")) ){
                    userLog.setUserID(line[0]);
                    context.write(new Text(userID), userLog);
                }
            }
        }
    }

    public static class MergeTableReducer
       extends Reducer<Text,UserLog,Text,UserLog> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<UserLog> values,
                        Context context
                        ) throws IOException, InterruptedException {
            UserLog userLog = new UserLog();
            for (UserLog obj : values) {
                userLog.setUserID(obj.getUserID);
                userLog.setSellerID(obj.getSellserID);
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