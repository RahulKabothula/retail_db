import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Test;

public class UtilTesting {
    @Test
    public void getSparkSessionTesting(){
        SparkSession sparkSession = Util.getSparkSession();
        SparkSession sparkSession1 = SparkSession
                .builder()
                .master("local")
                .getOrCreate();
        Assert.assertEquals(sparkSession,sparkSession1);
    }
}
