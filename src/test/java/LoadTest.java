import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Test;

public class LoadTest {

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
        Assert.assertEquals(4696,count);
    }

    @Test
    public void test_task2_count(){
        long count = datasets
                .getDatasets(format,task2_path)
                .count();
        Assert.assertEquals(7739,count);
    }

    @Test
    public void test_task3_count(){
        long count = datasets
                .getDatasets(format,task3_path)
                .count();
        Assert.assertEquals(12435,count);
    }

    @Test
    public void test_task4_count(){
        long count = datasets
                .getDatasets(format,task4_path)
                .count();
        Assert.assertEquals(33,count);
    }

    @Test
    public void test_task5_count(){
        long count = datasets
                .getDatasets(format,task5_path)
                .count();
        Assert.assertEquals(6,count);
    }
}
