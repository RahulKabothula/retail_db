import org.apache.spark.internal.config.R;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

//  import javax.xml.crypto.Data;


//  Usecase 1 - Product Count Per Department
//  Get the products for each department.
//  * Data should be sorted in ascending order by department_id
//  * Output should contain all the fields from department and the product count as product_count


public class Transform {

    public Dataset<Row>  task1(Dataset<Row> dept_df, Dataset<Row> cat_df, Dataset<Row> prod_df){

        return dept_df
                .join(cat_df,cat_df.col("category_department_id").equalTo(dept_df.col("department_id")),"inner")
                .join(prod_df,prod_df.col("product_category_id").equalTo(cat_df.col("category_id")),"inner")
                .groupBy("department_id","department_name")
                .count()
                .withColumnRenamed("count","product_count")
                .orderBy("department_id");
    }

//    Usecase 2 - Customer order count
//    Get order count per customer for the month of 2014 January.
//    * Tables - orders and customers
//    * Data should be sorted in descending order by count and ascending order by customer id.
//    * Output should contain customer_id, customer_first_name, customer_last_name and customer_order_count.

    public Dataset<Row> task2(Dataset<Row> customers_df,Dataset<Row> orders_df){
        return customers_df
                .join(orders_df,customers_df.col("customer_id").equalTo(orders_df.col("order_customer_id")),"inner")
                .filter(orders_df.col("order_date").substr(0,7).equalTo("2014-01"))
                .select(customers_df.col("customer_id"),customers_df.col("customer_fname"),customers_df.col("customer_lname"))
                .groupBy(customers_df.col("customer_id"),customers_df.col("customer_fname"),customers_df.col("customer_lname"))
                .count()
                .withColumnRenamed("count","customer_order_count")
                .orderBy(org.apache.spark.sql.functions.col("customer_order_count").desc(),org.apache.spark.sql.functions.col("customer_id").asc());
    }

//    Usecase 3 - Dormant Customers
//    Get the customer details who have not placed any order for the month of 2014 January.
//  * Tables - orders and customers
//  * Data should be sorted in ascending order by customer_id
//  * Output should contain all the fields from customers

    public Dataset<Row> task3(Dataset<Row> customers_df, Dataset<Row> orders_df){
        return orders_df
                .filter(orders_df.col("order_date").substr(0,7).equalTo("2014-01"))
                .join(customers_df,customers_df.col("customer_id").equalTo(orders_df.col("order_customer_id")),"rightouter")
                .filter(orders_df.col("order_customer_id").isNull())
                .drop("order_id","order_date","order_customer_id","order_status")
                .orderBy(customers_df.col("customer_id"));
    }

//    Usecase 4 - Revenue Per Customer
//    Get the revenue generated by each customer for the month of 2014 January
//    * Tables - orders, order_items and customers
//    * Data should be sorted in descending order by revenue and then ascending order by customer_id
//    * Output should contain customer_id, customer_first_name, customer_last_name, customer_revenue.
//    * If there are no orders placed by customer, then the corresponding revenue for a give customer should be 0.
//    * Consider only COMPLETE and CLOSED orders

    public Dataset<Row> task4(Dataset<Row> customer_df, Dataset<Row> order_df, Dataset<Row> order_item_df){
        return order_df
                .join(order_item_df,order_df.col("order_id").equalTo(order_item_df.col("order_item_order_id")),"inner")
                .filter(order_df.col("order_date").substr(0,7).equalTo("2014-01"))
                .filter(order_df.col("order_status").isin("COMPLETE","CLOSED"))
                .join(customer_df,customer_df.col("customer_id").equalTo(order_df.col("order_customer_id")),"rightouter")
                .select(customer_df.col("customer_id"),
                        customer_df.col("customer_fname"),
                        customer_df.col("customer_lname"),
                        functions.when(functions.col("order_item_subtotal").isNull(),0)
                                .otherwise(functions.col("order_item_subtotal")).as("order_item_subtotal"))
                .groupBy(functions.col("customer_id"),functions.col("customer_fname"),functions.col("customer_lname"))
                .sum("order_item_subtotal")
                .withColumnRenamed("sum(order_item_subtotal)","customer_revenue")
                .orderBy(functions.col("customer_revenue").desc(),functions.col("customer_id").asc());
    }

//    Usecase 5 - Revenue Per Category
//    Get the revenue generated for each category for the month of 2014 January
//    * Tables - orders, order_items, products and categories
//    * Data should be sorted in ascending order by category_id.
//    * Output should contain all the fields from category along with the revenue as category_revenue.
//    * Consider only COMPLETE and CLOSED orders
    public Dataset<Row> task5(Dataset<Row> category_df, Dataset<Row> products_df, Dataset<Row> order_item_df, Dataset<Row> orders_df){
        return category_df
                .join(products_df,category_df.col("category_id").equalTo(products_df.col(("product_category_id"))),"inner")
                .join(order_item_df,products_df.col("product_id").equalTo(order_item_df.col("order_item_product_id")),"inner")
                .join(orders_df,orders_df.col("order_id").equalTo(order_item_df.col("order_item_order_id")),"inner")
                .filter(orders_df.col("order_date").substr(0,7).equalTo("2014-01"))
                .filter(orders_df.col("order_status").isin("COMPLETE","CLOSED"))
                .groupBy(category_df.col("category_id"),category_df.col("category_department_id"),category_df.col("category_name"))
                .agg(functions.sum("order_item_subtotal").as("category_revenue"))
                .orderBy(functions.col("category_id"));
    }
}
