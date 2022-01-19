import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Datasets {
    SparkSession sparkSession = Util.getSparkSession();
    Extract extract = new Extract();

    //reading data
    public Dataset<Row> getDatasets(String src_file_format, String src_file_path){
        return extract.readData(sparkSession,src_file_format,src_file_path);
    }
}
