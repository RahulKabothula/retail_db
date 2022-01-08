import org.apache.spark.sql.SparkSession;

public class Util {

    public static SparkSession getSparkSession(){
        return SparkSession
                .builder()
                //.config("spark.sql.orc.impl","native")
                //.config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
                // .config("dfs.client.write.shortcircuit.skip.checksum", "true")
                .master("local[*]")
                .getOrCreate();
    }
}
