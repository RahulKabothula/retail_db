import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Load {

    public void write_data(Dataset<Row> df,String mode,String format,String compression, String trg_file_path){
        df.coalesce(1).write()
                .mode("overwrite")
                .option("header","true")
                .format("csv")
                .save(trg_file_path);
    }
}




