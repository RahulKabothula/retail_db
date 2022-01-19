import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;

public class Extract {

    public Dataset<Row> readData(SparkSession sparkSession,String srcFileFormat, String srcFilePath){
        return sparkSession
                .read()
                .format(srcFileFormat)
                .option("header",true)
                .option("inferSchema",true)
                .load(srcFilePath);
    }
}
