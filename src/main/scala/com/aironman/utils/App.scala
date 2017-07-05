package com.aironman.utils

import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Hello world!
 *
 */
object App extends App {

  def loadFile(sqlContext: SQLContext, pathCSV: String, nullValue: String, separator: String, customSchema: StructType, haveSchema: String): DataFrame = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", haveSchema) // Use first line of all files as header
    .option("delimiter", separator)
    .option("nullValue", nullValue)
    //Esto provoca que pete en runtime si encuentra un fallo en la línea que esté parseando
    .option("mode", "FAILFAST")
    .schema(customSchema)
    .load(pathCSV)

  def createParquetFile(df: DataFrame, pathParquet: String, saveMode: SaveMode, header: String, nullValue: String, delimiter: String): Unit = {

    df.write
      .format("com.databricks.spark.csv")
      .option("header", header)
      .option("delimiter", delimiter)
      .option("nullValue", nullValue)
      .mode(saveMode)
      //by default, gzip. Another values are uncompressed, snappy, gzip, lzo. This can be changed only at sqlContext Level.
      //Configuration of Parquet can be done using the setConf method on SQLContext or by running SET key=value commands using SQL.
      //.option("codec","spark.sql.parquet.compression.codec" + compression_codec)
      .parquet(pathParquet)
  }

  // Create Context
  val conf = new SparkConf()
  if (System.getProperty("spark.master") == null) conf.setMaster("local[2]")
  if (System.getProperty("spark.app.name") == null) conf.setAppName("parquetGenerator")

  //conf.set("spark.io.compression.codec","lzf")
  val sc = new SparkContext(conf)

  val sqlContext = new SQLContext(sc)

  val path_input_csv: String = args(0).split("=")(1)

  val path_output_parquet: String = args(1).split("=")(1)

  val schema_parquet : String = args(2).split("=")(1)

  val nullValue: String = args(3).split("=")(1)

  val separator : String = args(4).split("=")(1)

  val header : String = args(5).split("=")(1)

  val path: Path = new Path(schema_parquet)
  val fileSystem = path.getFileSystem(sc.hadoopConfiguration)

  val inputStream: FSDataInputStream = fileSystem.open(path)

  val schema_json = Stream.cons(inputStream.readLine(), Stream.continually(inputStream.readLine))

  val mySchemaStructType = DataType.fromJson(schema_json.head).asInstanceOf[StructType]

  val myDF : org.apache.spark.sql.DataFrame = loadFile(sqlContext, path_input_csv, nullValue, separator, mySchemaStructType, header)

  val saveMode = SaveMode.Append

  createParquetFile(myDF, path_output_parquet, saveMode, header, nullValue, separator)


}
