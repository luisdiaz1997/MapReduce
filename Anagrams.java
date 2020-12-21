import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.commons.lang.StringUtils;
import java.util.Arrays;

public class Anagrams {

    public static class TokenizerTextMapper extends Mapper<Object, Text, Text, Text> {

        private Text ordered= new Text();
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String pWord = itr.nextToken();
                if ((pWord.length() > 1) && (StringUtils.isAlpha(pWord)))
                {
                    pWord = pWord.toLowerCase();
                    char[] arr = pWord.toCharArray();
                    Arrays.sort(arr);
                    String orderedWord = new String(arr);
                    ordered.set(orderedWord);
                    word.set(pWord);

                    context.write(ordered, word);
                }

            }
        }
    }
    public static class Combiner extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<Text> uniques = new HashSet<Text>();
            for (Text value : values) {
                if (uniques.add(value)) {
                    context.write(key, value);
                }
            }
        }
    }

    public static class AnagramReducer extends Reducer<Text, Text, Text, Text> {
        private Text anagram = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String currentAnagrams = null;
            for (Text val : values) {
                if (currentAnagrams == null)
                {
                    currentAnagrams = val.toString();
                }
                else{
                    StringTokenizer itr = new StringTokenizer(val.toString(), ",");
                    while (itr.hasMoreTokens()) {
                        String currVal = itr.nextToken();
                        if (!currentAnagrams.contains(currVal)) {
                            currentAnagrams += ',' + currVal;
                        }
                    }
                }
            }
            if (currentAnagrams.split(",").length > 1)
            {
                anagram.set(currentAnagrams);
                context.write(key, anagram);
            }

        }
    }

    public static class FilterSmall extends Reducer<Text, Text, Text, Text> {
        private Text anagram = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String currentAnagrams = null;
            for (Text val : values) {
                if (currentAnagrams == null)
                {
                    currentAnagrams = val.toString();
                }
                else{
                    StringTokenizer itr = new StringTokenizer(val.toString(), ",");
                    while (itr.hasMoreTokens()) {
                        String currVal = itr.nextToken();
                        if (!currentAnagrams.contains(currVal)) {
                            currentAnagrams += ',' + currVal;
                        }
                    }
                }
            }
            anagram.set(currentAnagrams);
            context.write(key, anagram);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "anagrams");
        job.setJarByClass(Anagrams.class);
        job.setMapperClass(TokenizerTextMapper.class);
        job.setCombinerClass(Combiner.class);
        job.setReducerClass(AnagramReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
