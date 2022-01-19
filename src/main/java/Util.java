import org.apache.spark.sql.SparkSession;

public class Util {
    // Utility classes should not have public constructors
    // Utility classes, which are collections of static members, are not meant to be instantiated.
    // Even abstract utility classes, which can be extended, should not have public constructors.
    private Util(){}

    public static SparkSession getSparkSession(){
        return SparkSession
                .builder()
                //.config("spark.sql.orc.impl","native")
                //.config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
                //.config("dfs.client.write.shortcircuit.skip.checksum", "true")
                .master("local[*]")
                .getOrCreate();
    }
}
