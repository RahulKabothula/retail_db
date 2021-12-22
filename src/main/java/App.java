import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;


public class App {

    public static void main(String[] args){
        System.out.println("Hello World");

        //Create SparkSession
        SparkSession sparkSession = Util.getSparkSession();

        //read data
        Read read = new Read();

        String dept_path = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\src\\main\\resources\\retail_db\\departments\\part-00000";
        String cat_path = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\src\\main\\resources\\retail_db\\categories\\part-00000";
        String prod_path = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\src\\main\\resources\\retail_db\\products\\part-00000";

        Dataset<Row> dept_df = read.readData(sparkSession,"csv", dept_path);
        Dataset<Row> cat_df = read.readData(sparkSession,"csv",cat_path);
        Dataset<Row> prod_df = read.readData(sparkSession,"csv",prod_path);

        //Transform data
        Transform transform = new Transform();
        Dataset<Row> tdf1 = transform.task1(sparkSession,dept_df,cat_df,prod_df);
        tdf1.show();

        //Load data
//        Load load = new Load();
//
//        String format = "csv";
//        String mode = "overwrite";
//        String compression = null;
//        String trg_path = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\Orders\\src\\main\\resources\\result\\retail_db";
//
//        load.write_data(tdf1, format,mode,compression,trg_path);
    }
}
