package master;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.util.Iterator;

public class VehicleTelematics {

    public static void main(String[] args) throws Exception {

        //Get args
        if (args.length < 2) {
            System.out.println("You need 2 args with input file and output folder");
            System.exit(1);
        }

        String inFilePath = args[0];
        String outFolder = args[1];

        // File names and paths
        String outFilePathSpeedRadar = outFolder + "/speedfines.csv";
        String outFilePathAverageSpeed = outFolder + "/avgspeedfines.csv";
        String outFilePathAccident = outFolder + "/accidents.csv";

        // Variables
        int Timestamp = 0;
        int VID = 1;
        int Spd = 2;
        int XWay = 3;
        int Lane = 4;
        int Dir = 5;
        int Seg = 6;
        int Pos = 7;

        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //Optimize the program for 3 task manager slots
        env.setParallelism(3);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        ///////////////////////////////////
        /////////// SpeedRadar ////////////
        ///////////////////////////////////

        // Read from source
        DataStream<String> dataStream = env.readTextFile(inFilePath);

        // DataStream to Tuple
        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> dataStreamTuple = dataStream.map(
                new MapFunction<String, Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> map(String in) throws Exception {
                        String[] fieldArray = in.split(",");
                        Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> out =
                                new Tuple8(Integer.parseInt(fieldArray[0]),
                                        Integer.parseInt(fieldArray[1]),
                                        Integer.parseInt(fieldArray[2]),
                                        Integer.parseInt(fieldArray[3]),
                                        Integer.parseInt(fieldArray[4]),
                                        Integer.parseInt(fieldArray[5]),
                                        Integer.parseInt(fieldArray[6]),
                                        Integer.parseInt(fieldArray[7]));
                        return out;
                    }
                });

        // Filter reports with more than 90 mph
        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> dataStreamFilter =
                dataStreamTuple.filter(new FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> in) throws Exception {
                return in.f2 > 90; }
        });

        //Select output in order
        DataStream<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> dataStreamOutSpeedRadar =
                dataStreamFilter.project(Timestamp,VID,XWay,Seg,Dir,Spd);

        ///////////////////////////////////
        /////// AverageSpeedControl ///////
        ///////////////////////////////////

        // Read again from source (the following filter overlaps with previous one)
        DataStream<String> dataStream2 = env.readTextFile(inFilePath);
        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> dataStreamTuple2 = dataStream2.map(
                new MapFunction<String, Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> map(String in) throws Exception {
                        String[] fieldArray = in.split(",");
                        Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> out =
                                new Tuple8(Integer.parseInt(fieldArray[0]),
                                        Integer.parseInt(fieldArray[1]),
                                        Integer.parseInt(fieldArray[2]),
                                        Integer.parseInt(fieldArray[3]),
                                        Integer.parseInt(fieldArray[4]),
                                        Integer.parseInt(fieldArray[5]),
                                        Integer.parseInt(fieldArray[6]),
                                        Integer.parseInt(fieldArray[7]));
                        return out;
                    }
                });

        // Filter reports inside segments 52 and 56
        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> segment5256Ini =
                dataStreamTuple2.filter(new FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> in) throws Exception {
                return in.f6 >= 52 && in.f6 <= 56;}
        });

        // Select useful variables
        DataStream<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> segments5256 =
                segment5256Ini.project(Timestamp,VID,XWay,Dir,Seg,Pos);

        // Create the key taking into account VID, XWay and Dir; Generate timestamps and watermarks
        // We avoid failing reports from other sensors in a different XWay or Dir
        KeyedStream<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>> keyedStreamSegments5256 =
                segments5256.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>(){
                    @Override
                    public long extractAscendingTimestamp(Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> element) {
                        return element.f0*1000;
                    }
                }).keyBy(new KeySelector<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>>() {
                    @Override
                    public Tuple3<Integer, Integer, Integer> getKey(Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> value) throws Exception {
                        return Tuple3.of(value.f1, value.f2, value.f3);
                    }
                });

        // Custom window function for average speed
        class AverageSpeed implements WindowFunction<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>,
                Tuple6<Integer, Integer, Integer, Integer, Integer, Double>,
                Tuple3<Integer, Integer, Integer>,
                TimeWindow> {
            @Override
            public void apply(Tuple3<Integer, Integer, Integer> key,
                              TimeWindow window,
                              Iterable<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> input,
                              Collector<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> out) throws Exception {

                Iterator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> iterator = input.iterator();

                int timeMin = 999999999;
                int posMin = 999999999;
                int timeMax = 0;
                int posMax = 0;
                int segMin = 999;
                int segMax = 0;

                // Min Position (closest to west), Max Position (closest to east), Min Time (start), Max Time (end), Min Segment and Max Segment
                while(iterator.hasNext()){
                    Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> next = iterator.next();
                    int ntime = next.f0;
                    int npos = next.f5;
                    int nseg = next.f4;

                    if (ntime <= timeMin){
                        timeMin = ntime;
                    }
                    if (npos <= posMin){
                        posMin = npos;
                    }
                    if (ntime>=timeMax){
                        timeMax = ntime;
                    }
                    if (npos >= posMax){
                        posMax = npos;
                    }
                    if (nseg <= segMin){
                        segMin = nseg;
                    }
                    if (nseg >= segMax){
                        segMax = nseg;
                    }
                }

                // If the car passes through the segments 52 and 56 get the average speed (distance/time)
                if (segMin == 52 && segMax==56) {
                    double speedMs = (float)(posMax - posMin) / (timeMax - timeMin);
                    double speedMhp = 2.23694 * speedMs;
                    double speedMhpRound = Math.round(speedMhp * 10000d) / 10000d;
                    if (speedMhp > 60) {
                        out.collect(new Tuple6<Integer, Integer, Integer, Integer, Integer, Double>(timeMin,timeMax,key.f0,key.f1,key.f2,speedMhpRound));
                    }
                }
            }
        }

        // Get all the reports for each car inside a Windows and apply the function
        SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> dataStreamOutAverageSpeed =
                keyedStreamSegments5256
                        .window(EventTimeSessionWindows.withGap(Time.seconds(120))) //Wait 120s max between reports
                        .apply(new AverageSpeed());

        ///////////////////////////////////
        ///////// AccidentReporter ////////
        ///////////////////////////////////

        // Parallelism=1 to keep the order of the reports (With Parallelism=3 we get some non-consecutive time accidents)
        env.setParallelism(1);

        // Read again from source (to avoid issues with previous WaterMarks and filter overlap, why the conflict¿?)
        DataStream<String> dataStream3 = env.readTextFile(inFilePath);

        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> dataStreamTuple3 = dataStream3.map(
                new MapFunction<String, Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> map(String in) throws Exception {
                        String[] fieldArray = in.split(",");
                        Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> out =
                                new Tuple8(Integer.parseInt(fieldArray[0]),
                                        Integer.parseInt(fieldArray[1]),
                                        Integer.parseInt(fieldArray[2]),
                                        Integer.parseInt(fieldArray[3]),
                                        Integer.parseInt(fieldArray[4]),
                                        Integer.parseInt(fieldArray[5]),
                                        Integer.parseInt(fieldArray[6]),
                                        Integer.parseInt(fieldArray[7]));
                        return out;
                    }
                });

        // Filter stopped cars
        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> stoppedCarsIni =
                dataStreamTuple3.filter(new FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> in) throws Exception {
                        return in.f2==0;}
                });

        // Select useful variables
        SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> stoppedCars =
                stoppedCarsIni.project(Timestamp,VID,XWay,Dir,Seg,Pos);

        // Create the key taking into account VID, XWay, Dir, Seg and Pos; Generate timestamps and watermarks
        // We avoid failing reports from other sensors in a different XWay, Dir, Seg or Pos
        KeyedStream<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>, Tuple5<Integer,Integer,Integer,Integer,Integer>> keyedStreamstoppedCars =
                stoppedCars.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>(){
                    @Override
                    public long extractAscendingTimestamp(Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> element) {
                        return element.f0*1000;
                    }
                }).keyBy(new KeySelector<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>, Tuple5<Integer, Integer, Integer,Integer,Integer>>() {
                    @Override
                    public Tuple5<Integer, Integer, Integer, Integer, Integer> getKey(Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> value) throws Exception {
                        return Tuple5.of(value.f1,value.f2,value.f3,value.f4,value.f5);
                    }
                });

        // Setting Parallelism again to 3 to increase performance
        env.setParallelism(3);

        // Custom window function for getting the accidents
        class Accident implements WindowFunction<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>,
                Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>,
                Tuple5<Integer, Integer, Integer, Integer, Integer>,
                GlobalWindow> {
            @Override
            public void apply(Tuple5<Integer, Integer, Integer, Integer, Integer> key,
                              GlobalWindow window,
                              Iterable<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> input,
                              Collector<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> out) throws Exception {

                Iterator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> iterator = input.iterator();

                int timeMin = 999999999;
                int timeMax = 0;
                int i = 0;

                // Count the number of reports inside the windows (sometimes the reports are less than 4)
                while(iterator.hasNext()){
                    Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> next = iterator.next();
                    int ntime = next.f0;
                    if (ntime <= timeMin){
                        timeMin = ntime;
                    }
                    if (ntime>=timeMax){
                        timeMax = ntime;
                    }
                    i++;
                }

                // Check if the reports are 4 and also consecutive (Time2 - Time1 = 90s)
                if ((i==4) && (timeMax-timeMin==90)) {
                    out.collect(new Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>
                            (timeMin,timeMax,key.f0,key.f1,key.f3,key.f2,key.f4));
                }
            }
        }

        // Get all the reports inside windows of 4 elements with overlapping and apply the function
        SingleOutputStreamOperator<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> dataStreamOutAccident =
                keyedStreamstoppedCars
                        .countWindow(4, 1)
                        .apply(new Accident());

        ///////////////////////////////////
        //////////// Outputs //////////////
        ///////////////////////////////////

        env.setParallelism(1);
        dataStreamOutSpeedRadar.writeAsCsv(outFilePathSpeedRadar, FileSystem.WriteMode.OVERWRITE);
        dataStreamOutAverageSpeed.writeAsCsv(outFilePathAverageSpeed, FileSystem.WriteMode.OVERWRITE);
        dataStreamOutAccident.writeAsCsv(outFilePathAccident, FileSystem.WriteMode.OVERWRITE);

        try {
            env.execute("Vehicule Telematics");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
