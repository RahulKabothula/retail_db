import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Load {

    public void writeData(Dataset<Row> df,String mode,String format,String compression, String trgFilePath){
        df.coalesce(1).write()
                .mode("overwrite")
                .option("header","true")
                .format("csv")
                .save(trgFilePath);
    }
}




