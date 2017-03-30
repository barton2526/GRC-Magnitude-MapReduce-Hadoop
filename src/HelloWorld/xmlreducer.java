package HelloWorld;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class xmlreducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    //Declaring first half of reducerMap
    private Map<Text, DoubleWritable> reducerMap;

    //Initializing the totalRAC var
    //private IntWritable totalRAC = new IntWritable(0);
    private double totalRAC = 0;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        //Second half of reducerMap declared
        //System.out.println("SETUP STARTED!");
        reducerMap = new HashMap<>();
    }

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        //Evaluating the totalRAC
        //System.out.println("totalRAC b4: " + totalRAC);
        Double valueRAC = new Double(0);

        for (Iterator<DoubleWritable> it = values.iterator(); it.hasNext(); )
            valueRAC += it.next().get();

        //Sending the key and corresponding calculated sum into a hashmap!
        try {
            totalRAC += valueRAC;
            //System.out.println(key + " :: " + valueRAC);
            reducerMap.put(new Text(key), new DoubleWritable(valueRAC));
        } catch(Exception e) {
            System.out.println("error: " + e.getMessage());
            System.out.println("key: " + key + " value: " + valueRAC);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //Passing in the kVal from the command line!
        Configuration passedNumWhitelistedProjects = context.getConfiguration();
        double numWhitelistedProjects = Double.parseDouble(passedNumWhitelistedProjects.get("numWhitelistedProjects"));

        try {
            //Stream really needs better formatting
            LinkedHashMap<Text, DoubleWritable> sortedMapContents = new LinkedHashMap<>();

            //Opening reducerMap (unsorted hashmap)
            sortedMapContents = reducerMap.entrySet()
                    //Beginning stream's horrible formatting
                    .stream()
                    //Descending (reversed) value sort
                    .sorted(Map.Entry.<Text, DoubleWritable>comparingByValue().reversed())
                    //output to LinkedHashMap to preserve sort ordering; Tried applying .limit(kVal) here but java complained, thus limit is enforced later.
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2)->e1, LinkedHashMap::new));

            //Declaring iterator
            Iterator<Map.Entry<Text, DoubleWritable>> iterator = sortedMapContents.entrySet().iterator();

            //Iterate over each entry within reducerMap
            while(iterator.hasNext()){
                //Catch any errors (however unlikely)
                try {
                    //Grab contents of currentLine (key, value)
                    Map.Entry<Text, DoubleWritable> currentLine = iterator.next();

                    //Calculating project magnitude
                    DoubleWritable Magnitude = new DoubleWritable(((currentLine.getValue()).get()/totalRAC)*((double)115000/numWhitelistedProjects));

                    //Outputting the <k,v> pair from currentLine.
                    context.write(currentLine.getKey(), Magnitude);
                } catch(Exception e) {
                    //If there's an error during iteration
                    System.out.println("error: " + e.getMessage());
                }
            }
        } catch(Exception e) {
            System.out.println("error: " + e.getMessage());
        }
    }
}
