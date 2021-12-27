import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


//### Exercise 5 - Product Count Per Department
//        Get the products for each department.
//        * Data should be sorted in ascending order by department_id
//        * Output should contain all the fields from department and the product count as product_count


public class Transform {

    public Dataset<Row>  task1(SparkSession sparkSession, Dataset<Row> dept_df, Dataset<Row> cat_df, Dataset<Row> prod_df){

        return dept_df
                .join(cat_df,cat_df.col("category_department_id").equalTo(dept_df.col("department_id")),"inner")
                .join(prod_df,prod_df.col("product_category_id").equalTo(cat_df.col("category_id")),"inner")
                .groupBy("department_id","department_name")
                .count()
                .withColumnRenamed("count","product_count")
                .orderBy("department_id");
    }

}
