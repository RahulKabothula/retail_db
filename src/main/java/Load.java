import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Load {

    public void write_data(Dataset<Row> df, String format,String mode,String compression,String trg_file_path){
        df.write()
                .format(format)
                .mode(mode)
                .option("compression",compression)
                .save(trg_file_path);
    }
}
