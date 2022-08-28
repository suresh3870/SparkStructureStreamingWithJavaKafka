import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.functions.*;

import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.types.DataTypes.DateType;
import static org.apache.spark.sql.types.DataTypes.StringType;


public class JoinNewsPrice {

    private static final SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("SparknewsPrice").config("spark.sql.shuffle.partitions",2).
            config("spark.sql.crossJoin.enabled", "true").getOrCreate();
    private static String tickerfile = "ticker_industry.txt";


    public static void main(String[] args) throws TimeoutException, StreamingQueryException {


        System.out.println("Reading ticker files");
        StructType schema_ind = new StructType()
                .add("ticker_ind", "string")
                .add("industry", "string");
        Dataset<Row> dfticker = sparkSession.read()
                .schema(schema_ind)
                .csv(tickerfile);
        //Dataset<Row> ticker = sparkSession.read().text(tickerfile);
        System.out.println(dfticker.collectAsList());
        Dataset<Row> price = sparkSession.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "stocktopic")
                .option("startingOffsets", "latest")
                .load();
        Dataset<Row> news = sparkSession.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "newstopic")
                .option("startingOffsets", "latest")
                .load();
        Dataset<Row> price1 = price.selectExpr("timestamp AS priceTime", "CAST(value AS STRING)");
        //{"ticker": "dono", "date": "2022-07-18", "price": 10.08 }
        StructType price_schema = new StructType().add("ticker", DataTypes.StringType).add("date", DataTypes.DateType).add("price", DataTypes.StringType);
        Dataset<Row> df_price = price1.select(functions.col("priceTime"), functions.from_json(functions.col("value"), price_schema).alias("data"));
        df_price = df_price.select("priceTime",  "data.*");
        Dataset<Row> new1 = news.selectExpr("timestamp AS newsTime", "CAST(value AS STRING)");
        StructType news_schema = new StructType().add("ticker1", DataTypes.StringType).add("date", DataTypes.DateType).add("news", DataTypes.StringType);
        Dataset<Row> df_news = new1.select(functions.col("newsTime"), functions.from_json(functions.col("value"), news_schema).alias("data"));
        df_news = df_news.select("newsTime",  "data.*");
        Dataset<Row> priceWithWatermark = df_price.withWatermark("priceTime", "2 hours");
        Dataset<Row> newsWithWatermark = df_news.withWatermark("newsTime", "3 hours");
        Dataset<Row> newsprice = priceWithWatermark.join(
                newsWithWatermark,
                expr(
                        "ticker = ticker1 AND " +
                                "newsTime >= priceTime AND " +
                                "newsTime <= priceTime + interval 1 hour "),
                "leftOuter"
        );
        Dataset<Row> newspricewithInd = newsprice.join(dfticker, expr(
                "ticker1 = ticker_ind" ),"leftOuter");
        //Dataset<Row> newpriceticker = newsprice.join(ticker,"ticker");

        StreamingQuery query = null;
        try {
            query = newspricewithInd.writeStream().format("console").option("truncate", "false").outputMode("append").start();
            //query = ticker.write().format("console");
            query.awaitTermination(1000000);
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
    }
}

