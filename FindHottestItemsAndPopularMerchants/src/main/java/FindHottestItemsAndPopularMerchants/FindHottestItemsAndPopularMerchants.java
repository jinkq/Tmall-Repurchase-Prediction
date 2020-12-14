// package RepurchasePrediction;


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
    private Path tempDir;

    // public class MyInverseMapper extends InverseMapper<Text, IntWritable, IntWritable, Text>

    public FindHottestItemsAndPopularMerchants(String inputPath, String outputPath, Configuration conf){
		this.inputPath = inputPath;
		this.outputPath = outputPath;
		this.conf = conf;
        this.outputPathPath = new Path(outputPath);
        this.tempDir = new Path("tmp-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
	}
    
    public void measureItemsPopularityJob( ) throws Exception {
        Job measureItemsPopularityJob= new Job();
        measureItemsPopularityJob.setJobName("measureItemsPopularityJob" );
        measureItemsPopularityJob.setJarByClass(FindHottestItems.class);
        measureItemsPopularityJob.setMapperClass(FindHottestItems.measureItemsPopularityMapper.class);
        measureItemsPopularityJob.setReducerClass(FindHottestItems.IntSumReducer.class);
        measureItemsPopularityJob.setOutputKeyClass(Text.class);
        measureItemsPopularityJob.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(measureItemsPopularityJob, new Path(inputPath));
        
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

        FileInputFormat.addInputPath(sortItemsPopularityJob, new Path(inputPath)); 
        // sortItemsPopularityJob.setInputFormatClass(SequenceFileInputFormat.class);  

        //delete existed output dir
        FileSystem fileSystem = outputPathPath.getFileSystem(conf);
        if (fileSystem.exists(outputPathPath)) {
            fileSystem.delete(outputPathPath, true);
        }
        FileOutputFormat.setOutputPath(sortItemsPopularityJob, new Path(outputPath+"/hottest items"));
        // FileOutputFormat.setOutputPath(sortItemsPopularityJob, outputPathPath);

        sortItemsPopularityJob.waitForCompletion(true);
    }

    public void mergeTableJob( ) throws Exception {
        Job mergeTableJob= new Job();
        mergeTableJob.setJobName("mergeTableJob" );
       mergeTableJob.setJarByClass(FindPopularMerchantsAmongYoung.class);
        mergeTableJob.setMapperClass(FindPopularMerchantsAmongYoung.MergeTableMapper.class);
        mergeTableJob.setMapOutputKeyClass(IntWritable.class);
        mergeTableJob.setMapOutputValueClass(Text.class);
        
        mergeTableJob.setReducerClass(FindPopularMerchantsAmongYoung.MergeTableReducer.class);
        mergeTableJob.setOutputKeyClass(Text.class);
        mergeTableJob.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(mergeTableJob, tempDir); 

        FileOutputFormat.setOutputPath(mergeTableJob, new Path(outputPath+"/popular merchants among young"));
        // FileOutputFormat.setOutputPath(sortItemsPopularityJob, outputPathPath);

        mergeTableJob.waitForCompletion(true);
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
        
        // driver.measureItemsPopularityJob();
        // driver.sortItemsPopularityJob();

        driver.mergeTableJob();

        FileSystem.get(conf).deleteOnExit(driver.tempDir);
    }
}