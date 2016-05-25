
package com.cascading;

import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

import java.util.Properties;

/**
 * A Cascading example to read a text file which contains user name and age details and remove the users whose age is more than or equals to 30
 */
public class Main {

    public static void main(String[] args) {

        //input and output path
        String inputPath = args[0];
        String outputPath = args[1];

        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, Main.class);

        //Create the source tap
        Fields fields = new Fields("userName", "age");
        Tap inTap = new Hfs(new TextDelimited(fields, true, "\t"), inputPath);

        //Create the sink tap
        Tap outTap = new Hfs(new TextDelimited(false, "\t"), outputPath, SinkMode.REPLACE);

        // Pipe to connect Source and Sink Tap
        Pipe dataPipe = new Each("data", new CustomFilter(Fields.ALL));

        HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);
        FlowDef flowDef = FlowDef.flowDef().addSource(dataPipe, inTap).addTailSink(dataPipe, outTap).setName("Hdfs Job");
        flowConnector.connect(flowDef).complete();
    }

    /**
     * This custom filter will remove all the users whose age is more than or equals to 30
     */
    public static class CustomFilter extends BaseOperation implements Filter {
        private static final long serialVersionUID = 1L;

        public CustomFilter(Fields fields) {
            super(1, fields);
        }

        @Override
        public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
            TupleEntry arguments = filterCall.getArguments();
            String age = arguments.getString(1).trim();
            return Integer.valueOf(age) >= 30;
        }
    }
}

