import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.slf4j.LoggerFactory;

public class App {

    public static void main(String[] args){

        org.slf4j.Logger logger = LoggerFactory.getLogger(App.class);
        logger.info("------------------Creating Spark Session----------------------");
        //Create SparkSession
        SparkSession sparkSession = Util.getSparkSession();

        logger.info("------------------Spark Session created successfully----------------------");
        logger.info("------------------Extracting data----------------------");
        //Extract data
        String deptPath = System.getenv("dept_path");
        String catPath = System.getenv("cat_path");
        String prodPath = System.getenv("prod_path");
        String customersPath = System.getenv("customers_path");
        String ordersPath = System.getenv("orders_path");
        String orderItemsPath = System.getenv("order_items_path");

        Extract extract = new Extract();

        Dataset<Row> deptDf = extract.readData(sparkSession,"csv", deptPath);
        Dataset<Row> catDf = extract.readData(sparkSession,"csv",catPath);
        Dataset<Row> prodDf = extract.readData(sparkSession,"csv",prodPath);
        Dataset<Row> customersDf = extract.readData(sparkSession,"csv",customersPath);
        Dataset<Row> ordersDf = extract.readData(sparkSession,"csv",ordersPath);
        Dataset<Row> orderItemsDf = extract.readData(sparkSession,"csv",orderItemsPath);
        logger.info("------------------Extracted Data Successfully----------------------");

        //Transform data
        logger.info("------------------Transforming data----------------------");
        Transform transform = new Transform();

        Dataset<Row> tdf1 = transform.task1(customersDf,ordersDf);
        Dataset<Row> tdf2 = transform.task2(customersDf,ordersDf);
        Dataset<Row> tdf3 = transform.task3(customersDf,ordersDf,orderItemsDf);
        Dataset<Row> tdf4 = transform.task4(catDf,prodDf,orderItemsDf,ordersDf);
        Dataset<Row> tdf5 = transform.task5(deptDf,catDf,prodDf);
        logger.info("------------------Transforming data Successfully----------------------");
        //Load data
        logger.info("------------------Loading data----------------------");
        String format = System.getenv("format");
        String mode = System.getenv("mode");
        String compression = System.getenv("compression");

        String task1TrgPath = System.getenv("task1_trg_path");
        String task2TrgPath = System.getenv("task2_trg_path");
        String task3TrgPath = System.getenv("task3_trg_path");
        String task4TrgPath = System.getenv("task4_trg_path");
        String task5TrgPath = System.getenv("task5_trg_path");

        Load load = new Load();

        load.writeData(tdf1,format,mode,compression,task1TrgPath);
        load.writeData(tdf2,mode,format,compression,task2TrgPath);
        load.writeData(tdf3,mode,format,compression,task3TrgPath);
        load.writeData(tdf4,mode,format,compression,task4TrgPath);
        load.writeData(tdf5,mode,format,compression,task5TrgPath);
        logger.info("------------------Loaded data Successfully----------------------");
    }
}

