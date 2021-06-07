import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, StringType, StructType}

package object schema {
  val txnSchema =
    new StructType()
      .add("results",ArrayType(
        new StructType()
          .add("user",
            new StructType()
              .add("gender",StringType)
              .add("name",
                new StructType()
                  .add("title",StringType)
                  .add("first",StringType)
                  .add("last",StringType))
              .add("location",
                new StructType()
                  .add("street",StringType)
                  .add("city",StringType)
                  .add("state",StringType)
                  .add("zip",StringType))
              .add("email",StringType)
              .add("username",StringType)
              .add("password",StringType)
              .add("salt",StringType)
              .add("md5",StringType)
              .add("sha1",StringType)
              .add("sha256",StringType)
              .add("registered",IntegerType)
              .add("dob",IntegerType)
              .add("phone",StringType)
              .add("cell",StringType)
              .add("PPS",StringType)
              .add("picture",
                new StructType()
                  .add("large",StringType)
                  .add("medium",StringType)
                  .add("thumbnail",StringType)
              ))))
      .add("nationality",StringType)
      .add("seed",StringType)
      .add("version",StringType)
      .add("tran_detail",
        new StructType()
          .add("tran_card_type",ArrayType(StringType))
          .add("tran_amount",DoubleType)
          .add("product_id",StringType))
}
