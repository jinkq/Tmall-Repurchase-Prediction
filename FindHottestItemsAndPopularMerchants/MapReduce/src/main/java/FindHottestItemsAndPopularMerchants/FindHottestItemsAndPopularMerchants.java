package FindHottestItemsAndPopularMerchants;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Random;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.fs.FileSystem;

public class FindHottestItemsAndPopularMerchants{

    private String inputPath;
	private String outputPath;
	private Configuration conf;
    private Path outputPathPath; //Path type
    private Path tempDir, tempDir2;

    public FindHottestItemsAndPopularMerchants(String inputPath, String outputPath, Configuration conf){
		this.inputPath = inputPath;
		this.outputPath = outputPath;
		this.conf = conf;
        this.outputPathPath = new Path(outputPath);
        this.tempDir = new Path("tmp-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
        this.tempDir2 = new Path("temp-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
	}
    
    public void measureItemsPopularityJob( ) throws Exception {
        Job measureItemsPopularityJob= new Job();
        measureItemsPopularityJob.setJobName("measureItemsPopularityJob" );
        measureItemsPopularityJob.setJarByClass(FindHottestItems.class);
        measureItemsPopularityJob.setMapperClass(FindHottestItems.measureItemsPopularityMapper.class);
        measureItemsPopularityJob.setReducerClass(FindHottestItems.IntSumReducer.class);
        measureItemsPopularityJob.setOutputKeyClass(Text.class);
        measureItemsPopularityJob.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(measureItemsPopularityJob, new Path(inputPath+"/user_log_format1.csv"));
        
        FileSystem fileSystem = tempDir.getFileSystem(conf);
        if (fileSystem.exists(tempDir)) {
            fileSystem.delete(tempDir, true);
        }
        FileOutputFormat.setOutputPath(measureItemsPopularityJob, tempDir);
        measureItemsPopularityJob.setOutputFormatClass(SequenceFileOutputFormat.class);

        measureItemsPopularityJob.waitForCompletion(true);
    }

    public void sortItemsPopularityJob( ) throws Exception {
        Job sortItemsPopularityJob= new Job();
        sortItemsPopularityJob.setJobName("sortItemsPopularityJob" );
        sortItemsPopularityJob.setJarByClass(FindHottestItems.class);
        sortItemsPopularityJob.setSortComparatorClass(FindHottestItems.IntWritableDecreasingComparator.class);  //排序改写成降序
        sortItemsPopularityJob.setMapperClass(InverseMapper.class);
        sortItemsPopularityJob.setMapOutputKeyClass(IntWritable.class);
        sortItemsPopularityJob.setMapOutputValueClass(Text.class);
        
        sortItemsPopularityJob.setReducerClass(FindHottestItems.SortReducer.class);
        sortItemsPopularityJob.setOutputKeyClass(Text.class);
        sortItemsPopularityJob.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(sortItemsPopularityJob, tempDir); 
        sortItemsPopularityJob.setInputFormatClass(SequenceFileInputFormat.class);  

        //delete existed output dir
        FileSystem fileSystem = outputPathPath.getFileSystem(conf);
        if (fileSystem.exists(outputPathPath)) {
            fileSystem.delete(outputPathPath, true);
        }
        FileOutputFormat.setOutputPath(sortItemsPopularityJob, new Path(outputPath+"/hottest items"));

        sortItemsPopularityJob.waitForCompletion(true);
    }

    public void mergeTableJob( ) throws Exception {
        Job mergeTableJob= new Job();
        mergeTableJob.setJobName("mergeTableJob" );
        mergeTableJob.setJarByClass(FindPopularMerchantsAmongYoung.class);
        mergeTableJob.setMapperClass(FindPopularMerchantsAmongYoung.MergeTableMapper.class);
        mergeTableJob.setMapOutputKeyClass(Text.class);
        mergeTableJob.setMapOutputValueClass(UserLog.class);
        
        mergeTableJob.setReducerClass(FindPopularMerchantsAmongYoung.MergeTableReducer.class);
        mergeTableJob.setOutputKeyClass(Text.class);
        mergeTableJob.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(mergeTableJob, new Path(inputPath)); 

        FileSystem fileSystem = tempDir.getFileSystem(conf);
        if (fileSystem.exists(tempDir)) {
            fileSystem.delete(tempDir, true);
        }
        
        FileOutputFormat.setOutputPath(mergeTableJob, tempDir);

        mergeTableJob.waitForCompletion(true);
    }

    public void measureMerchantsPopularityJob( ) throws Exception {
        Job measureMerchantsPopularityJob= new Job();
        measureMerchantsPopularityJob.setJobName("measureMerchantsPopularityJob" );
        measureMerchantsPopularityJob.setJarByClass(FindPopularMerchantsAmongYoung.class);
        measureMerchantsPopularityJob.setMapperClass(FindPopularMerchantsAmongYoung.MeasureMerchantsPopularityMapper.class);
        measureMerchantsPopularityJob.setMapOutputKeyClass(Text.class);
        measureMerchantsPopularityJob.setMapOutputValueClass(IntWritable.class);
        
        measureMerchantsPopularityJob.setReducerClass(FindHottestItems.IntSumReducer.class);
        measureMerchantsPopularityJob.setOutputKeyClass(Text.class);
        measureMerchantsPopularityJob.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(measureMerchantsPopularityJob, tempDir); 
        // sortItemsPopularityJob.setInputFormatClass(SequenceFileInputFormat.class);  

        
        FileOutputFormat.setOutputPath(measureMerchantsPopularityJob, tempDir2);
        measureMerchantsPopularityJob.setOutputFormatClass(SequenceFileOutputFormat.class);

        measureMerchantsPopularityJob.waitForCompletion(true);
    }

    public void sortMerchantsPopularityJob( ) throws Exception {
        Job sortMerchantsPopularityJob= new Job();
        sortMerchantsPopularityJob.setJobName("sortMerchantsPopularityJob" );
        sortMerchantsPopularityJob.setJarByClass(FindPopularMerchantsAmongYoung.class);
        sortMerchantsPopularityJob.setSortComparatorClass(FindHottestItems.IntWritableDecreasingComparator.class);  //排序改写成降序
        sortMerchantsPopularityJob.setMapperClass(InverseMapper.class);
        sortMerchantsPopularityJob.setMapOutputKeyClass(IntWritable.class);
        sortMerchantsPopularityJob.setMapOutputValueClass(Text.class);
        
        sortMerchantsPopularityJob.setReducerClass(FindPopularMerchantsAmongYoung.SortReducer.class);
        sortMerchantsPopularityJob.setOutputKeyClass(Text.class);
        sortMerchantsPopularityJob.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(sortMerchantsPopularityJob, tempDir2); 
        sortMerchantsPopularityJob.setInputFormatClass(SequenceFileInputFormat.class);  

        FileOutputFormat.setOutputPath(sortMerchantsPopularityJob, new Path(outputPath+"/popular merchants among young"));

        sortMerchantsPopularityJob.waitForCompletion(true);
    }

    public static void main( String[] args ) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if (remainingArgs.length != 2) {
            System.err.println("Usage: FindHottestItemsAndPopularMerchants <in> <out>");
            System.exit(2);
        }

        List<String> otherArgs = new ArrayList<String>();
        for (int i=0; i < remainingArgs.length; ++i) {
            otherArgs.add(remainingArgs[i]);
        }

        FindHottestItemsAndPopularMerchants driver = new FindHottestItemsAndPopularMerchants(otherArgs.get(0), otherArgs.get(1), conf);
        
        //最热门商品
        driver.measureItemsPopularityJob();
        driver.sortItemsPopularityJob();

        //最受年轻人关注的商家
        driver.mergeTableJob();
        driver.measureMerchantsPopularityJob();
        driver.sortMerchantsPopularityJob();

        FileSystem.get(conf).deleteOnExit(driver.tempDir);
        FileSystem.get(conf).deleteOnExit(driver.tempDir2);
    }
}