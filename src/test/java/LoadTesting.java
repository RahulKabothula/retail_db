import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Test;

public class LoadTesting {

    Datasets datasets = new Datasets();
    Load load = new Load();
    String format = "csv";
    String task1_path = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\result\\task1_result\\part*";
    String task2_path = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\result\\task2_result\\part*";
    String task3_path = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\result\\task3_result\\part*";
    String task4_path = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\result\\task4_result\\part*";
    String task5_path = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\result\\task5_result\\part*";

    @Test
    public void test_task1_count(){
        long count = datasets
                .getDatasets(format,task1_path)
                .count();
        Assert.assertEquals(count,4696);
    }

    @Test
    public void test_task2_count(){
        long count = datasets
                .getDatasets(format,task2_path)
                .count();
        Assert.assertEquals(count,7739);
    }

    @Test
    public void test_task3_count(){
        long count = datasets
                .getDatasets(format,task3_path)
                .count();
        Assert.assertEquals(count,12435);
    }

    @Test
    public void test_task4_count(){
        long count = datasets
                .getDatasets(format,task4_path)
                .count();
        Assert.assertEquals(count,33);
    }

    @Test
    public void test_task5_count(){
        long count = datasets
                .getDatasets(format,task5_path)
                .count();
        Assert.assertEquals(count,6);
    }
}
