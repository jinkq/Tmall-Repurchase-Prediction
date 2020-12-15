// package RepurchasePrediction;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.ArrayList;

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
       extends Reducer<Text,UserLog,Text,NullWritable> {

        @Override
        public void reduce(Text key, Iterable<UserLog> values,
                        Context context
                        ) throws IOException, InterruptedException {
            UserLog userLog = new UserLog();
            List<String> sellers = new ArrayList<String>();
            int i = 0;
            for (UserLog obj : values) {
                if(i == 0)
                {
                    userLog.setUserID(obj.getUserID);
                }
                if(obj.getSellserID){
                    userLog.setSellerID(obj.getSellserID);
                    sellers.add(obj.getSellserID);
                }
                i++;
            }
            for(String seller:sellers){
                context.write(new Text(seller), NullWritable.get());
            }
        }
    }
}    