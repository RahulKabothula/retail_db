import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;

public class Extract {
    public Dataset<Row> readData(SparkSession sparkSession,String src_file_format, String src_file_path){
        return sparkSession
                .read()
                .format(src_file_format)
                .option("header",true)
                .option("inferSchema",true)
                .load(src_file_path);
    }
}
