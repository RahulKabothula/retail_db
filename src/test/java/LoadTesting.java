import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Test;

public class LoadTesting {

    Datasets datasets = new Datasets();
    Load load = new Load();
    String format = "csv";
    String task1_path = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\result\\task1_result\\part-00000-5829785c-adca-4786-8d0d-21feca356d52-c000.csv";
    String task2_path = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\result\\task2_result\\part-00000-845058cd-9c46-483f-a8cd-3cb7d2f3bd58-c000.csv";
    String task3_path = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\result\\task3_result\\part-00000-aec82ec4-b2f3-48f9-a1d0-d6347c2512db-c000.csv";
    String task4_path = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\result\\task4_result\\part-00000-f9629e6e-be08-46f9-a4be-ce3c8b4130fe-c000.csv";
    String task5_path = "C:\\Users\\Rahul Kabothula\\Intellij Maven Projects\\retail_db\\result\\task5_result\\part-00000-77fd9dfb-f29e-40d8-bd6e-873b382cc525-c000.csv";

    @Test
    public void test_task1_count(){
        long count = datasets
                .getDatasets(format,task1_path)
                .count();
        Assert.assertEquals(count,6);
    }

    @Test
    public void test_task2_count(){
        long count = datasets
                .getDatasets(format,task2_path)
                .count();
        Assert.assertEquals(count,4696);
    }

    @Test
    public void test_task3_count(){
        long count = datasets
                .getDatasets(format,task3_path)
                .count();
        Assert.assertEquals(count,7739);
    }

    @Test
    public void test_task4_count(){
        long count = datasets
                .getDatasets(format,task4_path)
                .count();
        Assert.assertEquals(count,12435);
    }

    @Test
    public void test_task5_count(){
        long count = datasets
                .getDatasets(format,task5_path)
                .count();
        Assert.assertEquals(count,33);
    }
}
