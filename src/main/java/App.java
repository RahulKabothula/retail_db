import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;

public class App {

    public static void main(String[] args){

        //Create SparkSession
        SparkSession sparkSession = Util.getSparkSession();

        //Extract data
        Extract extract = new Extract();

        String dept_path = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\src\\main\\resources\\retail_db\\departments\\part-00000";
        String cat_path = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\src\\main\\resources\\retail_db\\categories\\part-00000";
        String prod_path = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\src\\main\\resources\\retail_db\\products\\part-00000";
        String customers_path = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\src\\main\\resources\\retail_db\\customers\\part-00000";
        String orders_path = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\src\\main\\resources\\retail_db\\orders\\part-00000";
        String order_items_path = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\src\\main\\resources\\retail_db\\order_items\\part-00000";

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
        Load load = new Load();

        String format = "csv";
        String mode = "overwrite";
        String compression = null;

        String task1_trg_path = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\result\\task1_result";
        String task2_trg_path = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\result\\task2_result";
        String task3_trg_path = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\result\\task3_result";
        String task4_trg_path = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\result\\task4_result";
        String task5_trg_path = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\result\\task5_result";

        load.write_data(tdf1,format,mode,compression,task1_trg_path);
        load.write_data(tdf2,mode,format,compression,task2_trg_path);
        load.write_data(tdf3,mode,format,compression,task3_trg_path);
        load.write_data(tdf4,mode,format,compression,task4_trg_path);
        load.write_data(tdf5,mode,format,compression,task5_trg_path);
    }
}

