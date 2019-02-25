package aggregation;

import io.netty.handler.codec.redis.RedisArrayAggregator;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.SparkSession;
import org.spark_project.guava.collect.Lists;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AggregationTest {

    private SparkSession spark;

    private Dataset<Employee> employeeDataset;

    public AggregationTest() {

        spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("spark_test")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
    }

    private void createDataset()
    {
        Employee e1 = Employee.create("A", "dep1");
        Employee e2 = Employee.create("B", "dep1");
        Employee e3 = Employee.create("C", "dep1");
        Employee e4 = Employee.create("D", "dep2");
        Employee e5 = Employee.create("E", "dep3");
        Employee e6 = Employee.create("F", "dep1");

        List<Employee> employees = Arrays.asList(e1, e2, e3, e4, e5, e6);

        employeeDataset = spark.createDataset(employees, Encoders.kryo(Employee.class));
    }

    public void execute()
    {
        KeyValueGroupedDataset<String, Employee> agg = employeeDataset
                .groupByKey((MapFunction<Employee, String>) e -> e.department, Encoders.STRING());

        Dataset<Aggregator> aggregatorDataset = agg.mapGroups((MapGroupsFunction<String, Employee, Aggregator>) Aggregator::new,
                Encoders.kryo(Aggregator.class));

        aggregatorDataset.collectAsList().forEach(a -> {
            System.out.println("key: " + a.getKey());
            a.getEmployees().forEach(System.out::println);
        });
    }

    public static void main(String[] args)
    {
        AggregationTest run = new AggregationTest();

        run.createDataset();

        final Instant start = Instant.now();

        run.execute();

        final Instant finish = Instant.now();

        System.out.println("Time elapsed: " + Duration.between(start, finish).toMillis() + " ms.");
    }
}
