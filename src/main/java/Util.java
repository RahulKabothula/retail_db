import org.apache.spark.sql.SparkSession;

public class Util {

    public static SparkSession getSparkSession(){
        return SparkSession
                .builder()
                .enableHiveSupport()
                .appName("Task-1")
                .master("local")
                .getOrCreate();
    }
}
