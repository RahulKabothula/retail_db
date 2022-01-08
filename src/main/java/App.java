import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;


public class App {

    public static void main(String[] args){

        //Create SparkSession
        SparkSession sparkSession = Util.getSparkSession();

        //Extract data
        String dept_path = System.getenv("dept_path");
        String cat_path = System.getenv("cat_path");
        String prod_path = System.getenv("prod_path");
        String customers_path = System.getenv("customers_path");
        String orders_path = System.getenv("orders_path");
        String order_items_path = System.getenv("order_items_path");

        Extract extract = new Extract();

        Dataset<Row> dept_df = extract.readData(sparkSession,"csv", dept_path);
        Dataset<Row> cat_df = extract.readData(sparkSession,"csv",cat_path);
        Dataset<Row> prod_df = extract.readData(sparkSession,"csv",prod_path);
        Dataset<Row> customers_df = extract.readData(sparkSession,"csv",customers_path);
        Dataset<Row> orders_df = extract.readData(sparkSession,"csv",orders_path);
        Dataset<Row> order_items_df = extract.readData(sparkSession,"csv",order_items_path);

        //Transform data
        Transform transform = new Transform();

        Dataset<Row> tdf1 = transform.task1(dept_df,cat_df,prod_df);
        Dataset<Row> tdf2 = transform.task2(customers_df,orders_df);
        Dataset<Row> tdf3 = transform.task3(customers_df,orders_df);
        Dataset<Row> tdf4 = transform.task4(customers_df,orders_df,order_items_df);
        Dataset<Row> tdf5 = transform.task5(cat_df,prod_df,order_items_df,orders_df);

        //Load data
        String format = System.getenv("format");
        String mode = System.getenv("mode");
        String compression = System.getenv("compression");

        String task1_trg_path = System.getenv("task1_trg_path");
        String task2_trg_path = System.getenv("task2_trg_path");
        String task3_trg_path = System.getenv("task3_trg_path");
        String task4_trg_path = System.getenv("task4_trg_path");
        String task5_trg_path = System.getenv("task5_trg_path");

        Load load = new Load();

        load.write_data(tdf1,format,mode,compression,task1_trg_path);
        load.write_data(tdf2,mode,format,compression,task2_trg_path);
        load.write_data(tdf3,mode,format,compression,task3_trg_path);
        load.write_data(tdf4,mode,format,compression,task4_trg_path);
        load.write_data(tdf5,mode,format,compression,task5_trg_path);
    }
}

