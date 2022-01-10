import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Test;

public class ExtractTesting {
    String deptPath = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\src\\main\\resources\\retail_db\\departments\\part-00000";
    String catPath = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\src\\main\\resources\\retail_db\\categories\\part-00000";
    String productPath = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\src\\main\\resources\\retail_db\\products\\part-00000";
    String customersPath = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\src\\main\\resources\\retail_db\\customers\\part-00000";
    String ordersPath = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\src\\main\\resources\\retail_db\\orders\\part-00000";
    String orderItemsPath = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\src\\main\\resources\\retail_db\\order_items\\part-00000";

    String format = "csv";

    Datasets datasets = new Datasets();

    @Test
    public void test_department_count(){
        Dataset<Row> orders_df = datasets.getDatasets(format,deptPath);
        long n_records = orders_df.count();
        Assert.assertEquals(n_records,6);
    }

    @Test
    public void test_category_count(){
        Dataset<Row> orders_df = datasets.getDatasets(format,catPath);
        long n_records = orders_df.count();
        Assert.assertEquals(n_records,58);
    }

    @Test
    public void test_product_count(){
        Dataset<Row> orders_df = datasets.getDatasets(format,productPath);
        long n_records = orders_df.count();
        Assert.assertEquals(n_records,1345);
    }

    @Test
    public void test_customers_count(){
        Dataset<Row> orders_df = datasets.getDatasets(format,customersPath);
        long n_records = orders_df.count();
        Assert.assertEquals(n_records,12435);
    }

    @Test
    public void test_orders_count(){
        Dataset<Row> orders_df = datasets.getDatasets(format,ordersPath);
        long n_records = orders_df.count();
        Assert.assertEquals(n_records,68883);
    }

    @Test
    public void test_orderItems_count(){
        Dataset<Row> orders_df = datasets.getDatasets(format,orderItemsPath);
        long n_records = orders_df.count();
        Assert.assertEquals(n_records,172198);
    }

    @Test
    public void test_complete_orderItems_count(){
        Dataset<Row> orders_df = datasets.getDatasets(format,ordersPath);
        long n_records = orders_df
                .filter(orders_df.col("order_status").equalTo("COMPLETE"))
                .count();
        Assert.assertEquals(n_records,22899);
    }
}
