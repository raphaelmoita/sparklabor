package org.moita;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Scanner;

public class Exercicie1 {

    private final SparkSession spark;

    private final String viewsFilePath;
    private final String chaptersFilePath;
    private final String titlesFilePath;

    public Exercicie1() {

        Logger.getLogger("org.apache").setLevel(Level.ERROR);

        spark = SparkSession
                .builder()
                .master("local")
                .appName("exercicie")
                .getOrCreate();

        viewsFilePath = this.getClass().getResource("/views-1.csv").getPath();
        chaptersFilePath = this.getClass().getResource("/chapters.csv").getPath();
        titlesFilePath = this.getClass().getResource("/titles.csv").getPath();

    }

    public void execute() {
        JavaPairRDD<Integer, Integer> viewsRdd = spark.read().textFile(viewsFilePath.replace("views-1.csv", "views-*.csv"))
                .toJavaRDD().mapToPair(new ViewConverter()).cache();

        JavaPairRDD<Integer, Integer> titlesRdd = spark.read().textFile(titlesFilePath)
                .toJavaRDD().mapToPair(new TitleConverter());

        JavaPairRDD<Integer, Integer> chapterRdd = spark.read().textFile(chaptersFilePath)
                .toJavaRDD().mapToPair(new ChapterConverter());
        // warm up
        JavaPairRDD<Integer, Integer> chapterPerCourseRdd = chapterRdd
                .mapToPair(value -> new Tuple2<>(value._2, 1))
                .reduceByKey((value1, value2) -> value1 + value2);

        // remove duplicate views
        viewsRdd = viewsRdd.distinct();

        // chapterId as key
        viewsRdd.mapToPair(row -> new Tuple2<>(row._2, row._1));

        // join by chapterID view and chapter Rdds
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinViewAndChapterRdd =
                viewsRdd.join(chapterRdd);

        JavaPairRDD<Tuple2<Integer, Integer>, Integer> userPerCourseCount = joinViewAndChapterRdd
                .mapToPair(row -> new Tuple2<>(new Tuple2<>(row._2._1, row._2._2), 1));

        userPerCourseCount = userPerCourseCount.reduceByKey((val1, val2) -> val1 + val2);

        JavaPairRDD<Integer, Integer> courseAndPointsRdd = userPerCourseCount
                .mapToPair(row -> new Tuple2<>(row._1._2, row._2));

        JavaPairRDD<Integer, Tuple2<Integer, Integer>> courseAndTotalViewRdd =
                courseAndPointsRdd.join(chapterPerCourseRdd);

        JavaPairRDD<Integer, Double> courseAndTotalPercentViewRdd =
                courseAndTotalViewRdd.mapValues(t -> (double) t._1 / t._2);

        JavaPairRDD<Integer, Long> ratedCoursesRdd = courseAndTotalPercentViewRdd.mapValues(d -> {
            if (d > 0.9) return 10L;
            if (d > 0.5) return 4L;
            if (d > 0.25) return 2L;
            return 0L;
        });

        ratedCoursesRdd = ratedCoursesRdd.reduceByKey((val1, val2) -> val1 + val2);

        ratedCoursesRdd.sortByKey().take(10).forEach(System.out::println);

        new Scanner(System.in).nextLine();
    }

    static class ViewConverter implements PairFunction<String, Integer, Integer> {
        @Override
        public Tuple2<Integer, Integer> call(String line) throws Exception {
            String[] cols = line.split(",");
            Integer userId = Integer.valueOf(cols[0]);
            Integer chapterId = Integer.valueOf(cols[1]);
            return new Tuple2<>(userId, chapterId);
        }
    }

    static class TitleConverter implements PairFunction<String, Integer, Integer> {
        @Override
        public Tuple2<Integer, Integer> call(String line) throws Exception {
            String[] cols = line.split(",");
            Integer courseId = Integer.valueOf(cols[0]);
            Integer title = Integer.valueOf(cols[1]);
            return new Tuple2<>(courseId, title);
        }
    }

    static class ChapterConverterMapFunction implements MapFunction<String, Tuple2<Integer, Integer>> {
        @Override
        public Tuple2<Integer, Integer> call(String line) throws Exception {
            String[] cols = line.split(",");
            Integer chapterId = Integer.valueOf(cols[0]);
            Integer courseId = Integer.valueOf(cols[1]);
            return new Tuple2<>(chapterId, courseId);
        }
    }

    static class ChapterConverter implements PairFunction<String, Integer, Integer> {
        @Override
        public Tuple2<Integer, Integer> call(String line) throws Exception {
            String[] cols = line.split(",");
            Integer chapterId = Integer.valueOf(cols[0]);
            Integer courseId = Integer.valueOf(cols[1]);
            return new Tuple2<>(chapterId, courseId);
        }
    }

    public static void main(String[] args) {
        new Exercicie1().execute();
    }
}
