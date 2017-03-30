package HelloWorld;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import static javax.xml.stream.XMLStreamConstants.CHARACTERS;
import static javax.xml.stream.XMLStreamConstants.START_ELEMENT;

public class XMLMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	@Override
	protected void map(LongWritable key, Text value, Mapper.Context context)
			throws IOException, InterruptedException {
		String document = value.toString();
		//System.out.println("key: " + key + " & Value: " + value);

        //System.out.println("VARS DECLARED");
        String propertyCPID = "";
        String propertyAVGRAC = "";
        String currentElement = "";

		try {
            XMLInputFactory inputFactory = XMLInputFactory.newInstance();
            XMLStreamReader reader = inputFactory.createXMLStreamReader(new ByteArrayInputStream(document.getBytes()));
		    //XMLStreamReader reader = XMLInputFactory.newInstance().createXMLStreamReader();
            //XMLInputFactory factory = XMLInputFactory.newInstance(document.getBytes());
            //XMLEventReader eventReader = factory.createXMLEventReader(new FileReader("input.txt"));
            while (reader.hasNext()) {
			    //System.out.println("READER INIT");
				int code = reader.next();
				//System.out.println(code);
				switch (code) {
                    case START_ELEMENT: // START_ELEMENT:
                        currentElement = reader.getLocalName();
                        break;
                    case CHARACTERS: // CHARACTERS:
                        if (currentElement.equalsIgnoreCase("cpid")) {
                            propertyCPID += reader.getText();
                            //System.out.println("CPID: " + propertyCPID);
                            break;
                        } else if (currentElement.equalsIgnoreCase("expavg_credit")) {
                            propertyAVGRAC += reader.getText();
                            //System.out.println("expAVG_Credit" + propertyAVGRAC);
                            //System.out.println("expAVG_Credit INT" + Integer.parseInt(propertyAVGRAC.trim()));
                            break;
                        } else {
                            //System.out.println("other: " + reader.getText());
                            break;
                        }
				}
			}
            try {
                //Min avgRAC of 1!
                if (Double.parseDouble(propertyAVGRAC.trim()) > ((double) 1)) {
                    //System.out.println("AVGRAC > 1");
                    DoubleWritable avgRAC = new DoubleWritable(Math.ceil(Double.parseDouble(propertyAVGRAC.trim())));
                    context.write(new Text(propertyCPID.trim()), avgRAC);
                }
            } catch (Exception e) {
                System.out.println("Context.write ERROR: " + e.getMessage());
            }
			reader.close();
			//Outputting record's <k,v> pair.

		} catch (Exception e) {
			System.out.println("Exception error: " + e.getMessage());
			throw new IOException(e);
		}

	}
}
