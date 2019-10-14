package top.zhaohaoren.flink.ch04;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataSetTransformationApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        mapFunction(env);
//        filterFunction(env);
//        mapPartitionFunction(env);
//        flatMapFunction(env);
//        joinFunction(env);
        outerJoinFunction(env);
    }


    // map的作用：将集合里面的每个元素都作用 指定的function
    private static void mapFunction(ExecutionEnvironment env) throws Exception {
        DataSource<Integer> source = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));
        source.map(x -> x + 1).print();
    }


    // filter作用：过滤出我想要的
    private static void filterFunction(ExecutionEnvironment env) throws Exception {
        DataSource<Integer> source = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));
        source.map(x -> x + 1).filter(x -> x > 5).print();
    }

    // map partition：将数据分区，然后将一个区中的数据作为整体 作用map函数
    private static void mapPartitionFunction(ExecutionEnvironment env) throws Exception {
        DataSource<Integer> source = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));
        // 这个下面会打印10次，对于数据库获取连接等操作是不允许的。
//        source.map(x -> {
//            System.out.println("获取一个连接");
//            return x;
//        }).print();
        // 设置并行度，设置几个并行读。就分成几个区。下面就打印几次
        source.setParallelism(4);
        source.mapPartition(new MapPartitionFunction<Integer, Integer>() {
            @Override
            public void mapPartition(Iterable<Integer> iterable, Collector<Integer> collector) {
                System.out.println("获取一个连接 ");
            }
        }).print();
    }

    // first作用：找出前几个
    private static void firstFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer, String>> data = new ArrayList<>();
        data.add(new Tuple2<>(1, "Spark"));
        data.add(new Tuple2<>(2, "Hadoop"));
        data.add(new Tuple2<>(1, "Storm"));
        data.add(new Tuple2<>(3, "Flink"));
        data.add(new Tuple2<>(1, "Java"));
        data.add(new Tuple2<>(3, "SpringBoot"));
        data.add(new Tuple2<>(4, "Java"));
        data.add(new Tuple2<>(1, "Go"));

        DataSource<Tuple2<Integer, String>> source = env.fromCollection(data);
        // 取前几个
        source.first(3).print();
        System.out.println("----------------------");
        // 输出所有的组，但是只输出组内2个元素
        source.groupBy(0).first(2).print();
        System.out.println("----------------------");
        // 组内排序，然后输出
        source.groupBy(0).sortGroup(1, Order.DESCENDING).first(2).print();
    }

    //flatMap： 将数组的二维数据，给降维到1维，全部在一个list里面操作
    private static void flatMapFunction(ExecutionEnvironment env) throws Exception {
        List<String> data = new ArrayList<>();
        data.add("java,scala");
        data.add("java,go");
        data.add("js,c");
        DataSource<String> source = env.fromCollection(data);
        // 实现一个word count
        source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] terms = s.split(",");
                for (String term : terms) {
                    collector.collect(term);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            // 我对于lambda理解很差，这地方我按照idea提示的为什么就不行了呢？
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        }).groupBy(0).sum(1).print();
    }

    // join
    private static void joinFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer, String>> people = new ArrayList<>();
        people.add(new Tuple2<>(1, "Justin"));
        people.add(new Tuple2<>(2, "Lucy"));
        people.add(new Tuple2<>(3, "James"));
        people.add(new Tuple2<>(4, "William"));
        List<Tuple2<Integer, String>> city = new ArrayList<>();
        city.add(new Tuple2<>(1, "北京"));
        city.add(new Tuple2<>(2, "上海"));
        city.add(new Tuple2<>(3, "广州"));
        city.add(new Tuple2<>(5, "深圳"));

        DataSource<Tuple2<Integer, String>> source1 = env.fromCollection(people);
        DataSource<Tuple2<Integer, String>> source2 = env.fromCollection(city);

        source1.join(source2).where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> t1, Tuple2<Integer, String> t2) throws Exception {
                        return new Tuple3<>(t1.f0, t1.f1, t2.f1);
                    }
                }).print();


    }

    // outer join
    private static void outerJoinFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer, String>> people = new ArrayList<>();
        people.add(new Tuple2<>(1, "Justin"));
        people.add(new Tuple2<>(2, "Lucy"));
        people.add(new Tuple2<>(3, "James"));
        people.add(new Tuple2<>(4, "William"));
        List<Tuple2<Integer, String>> city = new ArrayList<>();
        city.add(new Tuple2<>(1, "北京"));
        city.add(new Tuple2<>(2, "上海"));
        city.add(new Tuple2<>(3, "广州"));
        city.add(new Tuple2<>(5, "深圳"));

        DataSource<Tuple2<Integer, String>> source1 = env.fromCollection(people);
        DataSource<Tuple2<Integer, String>> source2 = env.fromCollection(city);

        source1.leftOuterJoin(source2).where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> t1, Tuple2<Integer, String> t2) throws Exception {
                        if (t2 == null) {
                            return new Tuple3<>(t1.f0, t1.f1, "-");
                        } else {
                            return new Tuple3<>(t1.f0, t1.f1, t2.f1);
                        }
                    }
                }).print();
    }

    // cross 笛卡尔积
    private static void crossFunction(ExecutionEnvironment env) throws Exception {
        List<String> data1 = new ArrayList<>();
        data1.add("A");
        data1.add("B");
        List<String> data2 = new ArrayList<>();
        data2.add("1");
        data2.add("2");
        data2.add("3");

        DataSource<String> s1 = env.fromCollection(data1);
        DataSource<String> s2 = env.fromCollection(data2);

        s1.cross(s1).print();
    }
}
