import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import java.util.*;

public class LeastFive{

    private static final int n = 5; //set globally, for easy testing
    /*
        This class is used to make use of the ArrayList class and its sorted
        structure. StringCount objects are equal if they contain the same
        string, but are compared by their count in a document.
     */
    public static class StringCount implements Comparable<StringCount>{
        private final String str;
        private int count;

        /*
            used for dictating how we count, if we want to
            get most count, the count will be negative
         */
        public StringCount(String str, int count){
            this.str = str;
            this.count = count;
        }

        /*
            Basic constructor for regular counting
         */
        public StringCount(String str){
            this.str = str;
            this.count = 1;
        }

        public int getCount() {
            return count;
        }

        public String toString(){
            return str;
        }

        public void increment(){
            count++;
        }

        public void increment(int i) {
            count += i;
        }

        //can be used for counting most usages
        public void decrement(){
            count--;
        }

        @Override
        public int compareTo(StringCount o) {
            return Integer.compare(this.getCount(), o.getCount());
        }

        @Override
        public boolean equals(Object obj) {
            if(obj.getClass() != StringCount.class){
                return false;
            }
            return this.toString().equals(obj.toString());
        }
    }

    public static class LeastFiveMapper extends Mapper<Object, Text, Text, IntWritable>{
        /*  While the solution provided uses a TreeMap, that would only be useful if the count we receive for each string
            is final, which it is not. So we need both keep counting for each individual String, and keep a record of
            the least 5. This mapper will grow its list with all the words and their count, before sorting and selecting the lowest
            5 words at the front. This is to prevent words with infrequent usage from entering and leaving the lowest list,
            when they are actually used multiple times.
         */
        private ArrayList<StringCount> list;

        @Override
        public void setup(Context context){
            list = new ArrayList<StringCount>();
        }

        @Override
        public void map(Object key, Text value, Context context){
            String [] line = value.toString().split(" "); //break string by spaces
            for(String s:line){
                StringCount temp = new StringCount(s); //count doesn't matter if it exists already, we only check equals on the String
                int i = list.indexOf(temp);
                if(i != -1){
                    list.get(i).increment();
                }
                else {
                    list.add(temp);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            list.sort(null); //sort ArrayList so we always have the most used words at the end
            // only grab the first, lowest 5 results
            int size = Math.min(n, list.size());
            for(int i = 0; i < size; i++){
                StringCount s = list.get(i);
                String str = s.toString();
                int count = s.getCount();
                context.write(new Text(str), new IntWritable(count));
            }
        }
    }

    public static class LeastFiveReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        /*
            While our reducer also grows with the words added, they will represent the final count of that word's usage
            after sorting. This is done by examining all of the lowest 5 from each mapper, and producing a master least 5 list
         */

        private ArrayList<StringCount> list;

        @Override
        protected void setup(Context context) {
            list = new ArrayList<>(n+1);
        }
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context){
            int count = 0;
            for(IntWritable i: values){
                count += i.get();
            }

            StringCount temp = new StringCount(key.toString(), count);
            int i = list.indexOf(temp);
            if(i != -1){
                list.get(i).increment(count);
            }
            else {
                list.add(temp);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            list.sort(null); //sort ArrayList so we always have the most used words at the end
            // only grab the first, lowest n results
            int size = Math.min(n, list.size());
            for(int i = 0; i < size; i++){
                StringCount s = list.get(i);
                String str = s.toString();
                int count = s.getCount();
                context.write(new Text(str), new IntWritable(count));
            }
        }
    }

    public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArg = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf, "Least Five");
        job.setJarByClass(LeastFive.class);
        job.setMapperClass(LeastFiveMapper.class);
        job.setReducerClass(LeastFiveReducer.class);
        job.setNumReduceTasks(1); //set to one, so we only get one global list of lowest 5
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArg[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArg[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}