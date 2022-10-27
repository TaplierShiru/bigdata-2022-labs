# Lab 3. Gribanov Danil 6133.
## Streaming processing in Apache Flink

All test passed!

## Exercise 1. RideCleansingExercise.
### Task
Filter a data stream of taxi ride records to keep only within New York City. The resulting stream should be printed.

### Solution

```java
private static class NYCFilter implements FilterFunction<TaxiRide> {

    @Override
    public boolean filter(TaxiRide taxiRide) throws Exception {
        return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) && GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
    }
}
```

### Explanation
In data-stream, we know geo-location of the taxi. With GeoUtils we can check if some get-location is withing New York.
Using this utils method, we can simple filter data.

## Exercise 2. RidesAndFaresExercise.
### Task
The goal for this exercise is to enrich TaxiRides with fare information, 
i.e. to join together the `TaxiRide` and `TaxiFare` records for each ride.

### Solution

```java
public static class EnrichmentFunction extends RichCoFlatMapFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {

    private ValueState<TaxiRide> taxiRideValueState;
    private ValueState<TaxiFare> taxiFareValueState;

    @Override
    public void open(Configuration config) throws Exception {
        ValueStateDescriptor<TaxiRide> taxiRideValueStateDescriptor = new ValueStateDescriptor<TaxiRide>(
                "persistedTaxiRide", TaxiRide.class
        );
        ValueStateDescriptor<TaxiFare> taxiFareValueStateDescriptor = new ValueStateDescriptor<TaxiFare>(
                "persistedTaxiFare", TaxiFare.class
        );

        this.taxiRideValueState = getRuntimeContext().getState(taxiRideValueStateDescriptor);
        this.taxiFareValueState = getRuntimeContext().getState(taxiFareValueStateDescriptor);
    }

    @Override
    public void flatMap1(TaxiRide ride, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
        TaxiFare taxiFare = this.taxiFareValueState.value();
        if (taxiFare != null) {
            this.taxiFareValueState.clear();
            out.collect(new Tuple2<>(ride, taxiFare));
        } else {
            this.taxiRideValueState.update(ride);
        }
    }

    @Override
    public void flatMap2(TaxiFare fare, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
        TaxiRide taxiRide = this.taxiRideValueState.value();
        if (taxiRide != null) {
            this.taxiRideValueState.clear();
            out.collect(new Tuple2<>(taxiRide, fare));
        } else {
            this.taxiFareValueState.update(fare);
        }
    }
}
```

### Explanation
In this exercise we get acquainted with `RichCoFlatMapFunction`.

Each element in the stream **needs** to have its event timestamp assigned.
This is usually done by accessing/extracting the timestamp. In our case its some START or\and END time.

In this exercise END time is ignored, and we should only join by START time with some `rideId` with its
matching `TaxiFare`.

In order to join them, `RichCoFlatMapFunction` created which are represents a FlatMap transformation 
with two different input types.

Via `ValueState<T>` we can keep value\update\retrieve.

## Exercise 3. HourlyTipsExercise.

### Task
The task of the "Hourly Tips" exercise is to identify, for each hour, the driver earning the most tips. 
It's easiest to approach this in two steps: first use hour-long windows that compute the total tips for each driver 
during the hour, and then from that stream of window results, find the driver with the maximum tip total for each hour.

### Solution

```java
public class HourlyTipsExercise extends ExerciseBase {

    public static void main(String[] args) throws Exception {

        // read parameters
        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", ExerciseBase.pathToFareData);

        final int maxEventDelay = 60;       // events are out of order by max 60 seconds
        final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(ExerciseBase.parallelism);

        // start the data generator
        DataStream<TaxiFare> fares = env.addSource(fareSourceOrTest(new TaxiFareSource(input, maxEventDelay, servingSpeedFactor)));
        // compute tips per hour for each driver
        DataStream<Tuple3<Long, Long, Float>> hourlyTips =
                fares.keyBy(fare -> fare.driverId)
                        .timeWindow(Time.hours(1))
                        .process(new CalculateHourlyTips());

        // find the driver with the highest sum of tips for each hour
        DataStream<Tuple3<Long, Long, Float>> hourlyMax =
                hourlyTips.timeWindowAll(Time.hours(1))
                        .maxBy(2);


        printOrTest(hourlyMax);

        // execute the transformation pipeline
        env.execute("Hourly Tips (java)");
    }

    private static class CalculateHourlyTips
            extends ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {

        @Override
        public void process(
                Long key,
                Context context,
                Iterable<TaxiFare> fares,
                Collector<Tuple3<Long, Long, Float>> out) {

            float tipsSum = 0.0f;
            for (TaxiFare fare : fares) {
                tipsSum += fare.tip;
            }
            out.collect(Tuple3.of(context.window().getEnd(), key, tipsSum));
        }
    }
}
```

### Explanation
The TaxiFareGenerator annotates the generated DataStream<TaxiFare> with timestamps and watermarks. 
Hence, there is no need to provide a custom timestamp and watermark assigner in order to correctly use event time.
So, we don't worry about these timestamp/watermarks, since exercise author's already assign required watermark.

## Exercise 4. ExpiringStateExercise

### Task
The goal for this exercise is to enrich TaxiRides with fare information.

### Solution

```java
public static class EnrichmentFunction extends KeyedCoProcessFunction<Long, TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {

    private ValueState<TaxiRide> taxiRideValueState;
    private ValueState<TaxiFare> taxiFareValueState;

    @Override
    public void open(Configuration config) throws Exception {
        ValueStateDescriptor<TaxiRide> taxiRideDescriptor = new ValueStateDescriptor<>(
                "persistedTaxiRide", TaxiRide.class
        );
        ValueStateDescriptor<TaxiFare> taxiFareDescriptor = new ValueStateDescriptor<>(
                "persistedTaxiFare", TaxiFare.class
        );

        this.taxiRideValueState = getRuntimeContext().getState(taxiRideDescriptor);
        this.taxiFareValueState = getRuntimeContext().getState(taxiFareDescriptor);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
        if (this.taxiFareValueState.value() != null) {
            ctx.output(unmatchedFares, this.taxiFareValueState.value());
            this.taxiFareValueState.clear();
        }
        if (this.taxiRideValueState.value() != null) {
            ctx.output(unmatchedRides, this.taxiRideValueState.value());
            this.taxiRideValueState.clear();
        }
    }

    @Override
    public void processElement1(TaxiRide ride, Context context, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
        TaxiFare fare = this.taxiFareValueState.value();
        if (fare != null) {
            this.taxiFareValueState.clear();
            context.timerService().deleteEventTimeTimer(ride.getEventTime());
            out.collect(new Tuple2<>(ride, fare));
        } else {
            this.taxiRideValueState.update(ride);
            context.timerService().registerEventTimeTimer(ride.getEventTime());
        }
    }

    @Override
    public void processElement2(TaxiFare fare, Context context, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
        TaxiRide ride = this.taxiRideValueState.value();
        if (ride != null) {
            this.taxiRideValueState.clear();
            context.timerService().deleteEventTimeTimer(fare.getEventTime());
            out.collect(new Tuple2<>(ride, fare));
        } else {
            this.taxiFareValueState.update(fare);
            context.timerService().registerEventTimeTimer(fare.getEventTime());
        }
    }
}
```

### Explanation
The ProcessFunction can be thought of as a FlatMapFunction with access to keyed state and timers.
It handles events by being invoked for each event received in the input stream(s).

KeyedProcessFunction, as an extension of ProcessFunction, gives access to the key of timers in its onTimer(...) method.

The TimerService deduplicates timers per key and timestamp, i.e., there is at most one timer per key and timestamp.
If multiple timers are registered for the same timestamp, the onTimer() method will be called just once.

So if there is _no ride_ for the target fare - we'll add that ride in the _unmatchedRides_ list. The same for fares.