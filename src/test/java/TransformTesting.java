import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Test;



public class TransformTesting {

    Datasets datasets = new Datasets();
    Transform transform = new Transform();
    String deptPath = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\src\\main\\resources\\retail_db\\departments\\part-00000";
    String catPath = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\src\\main\\resources\\retail_db\\categories\\part-00000";
    String productPath = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\src\\main\\resources\\retail_db\\products\\part-00000";
    String customersPath = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\src\\main\\resources\\retail_db\\customers\\part-00000";
    String ordersPath = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\src\\main\\resources\\retail_db\\orders\\part-00000";
    String orderItemsPath = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\src\\main\\resources\\retail_db\\order_items\\part-00000";
    String format = "csv";

    //check count for all tasks
    @Test
    public void task1Testing(){
        // getting all the datasets
        Dataset<Row> deptDF = datasets.getDatasets(format,deptPath);
        Dataset<Row> catDF = datasets.getDatasets(format,catPath);
        Dataset<Row> prodDF = datasets.getDatasets(format,productPath);
        Dataset<Row> customersDF = datasets.getDatasets(format,customersPath);
        Dataset<Row> ordersDF = datasets.getDatasets(format,ordersPath);
        Dataset<Row> order_itemsDF = datasets.getDatasets(format,orderItemsPath);

        //checking task5 count
        long count = transform
                .task5(deptDF,catDF,prodDF)
                .count();
        Assert.assertEquals(count,6);

        //checking task1 count
        count = transform
                .task1(customersDF,ordersDF)
                .count();
        Assert.assertEquals(count,4696);

        //checking task2 count
        count = transform
                .task2(customersDF,ordersDF)
                .count();
        Assert.assertEquals(count,7739);

        //checking task3 count
        count = transform
                .task3(customersDF,ordersDF,order_itemsDF)
                .count();
        Assert.assertEquals(count,12435);

        //checking task4 count
        count = transform
                .task4(catDF,prodDF,order_itemsDF,ordersDF)
                .count();
        Assert.assertEquals(count,33);
    }


}
