import org.apache.spark.sql.functions._

val colunasVpFull = Seq(
  "vp032_cp_rpl",   "vp259_cp_rpl", "vpdcm0023_rpl", "vp008_cp_rpl",  "vp18_rpl",
  "vp089_cp_rpl",   "vp257_cp_rpl", "vp211_cp_rpl",  "vp244_cp_rpl",  "vpdcr0037_rpl",
  "vp036_cp_rpl",   "vp043_cp_rpl", "vp172_cp_rpl",  "vp243_cp_rpl",  "vpccf0009_rpl",
  "vp146_cp_rpl",   "vp002_cp_rpl", "vpdcm0035_rpl", "vpdcr0016_rpl", "vp49_rpl",
  "vp054_cp_rpl",   "vp232_cp_rpl", "vp137_rpl",     "vp136_rpl",     "vpdcr0006_rpl",
  "vpdcs0039_rpl",  "vp082_cp_rpl", "vp161_rpl",     "vp256_cp_rpl",  "vp307_cp_rpl",
  "vp039_cp_rpl",   "vp021_cp_rpl", "vpdcr0035_rpl", "vp148_cp_rpl",  "vp46_rpl",
  "pub_1_flag_rpl", "vp201_cp_rpl", "vp209_cp_rpl",  "vp266_cp_rpl",  "vp47_rpl",
  "vp29_rpl",       "vpdcr0003_rpl","vp117_cp_rpl",  "vp015_cp_rpl",  "vp095_cp_rpl",
  "vp202_cp_rpl",   "vp197_cp_rpl"
)

val colunasVp = Seq("vp1", "vp2", "vp3")


//  date_format(date_sub(current_date(), (rand() * 365).cast("int")), "yyyyMMdd").as("movimento")
val expressoes = Seq(
  lpad((rand() * 100000000000L).cast("long").cast("string"), 11, "0").as("chave"),
  lit("20250424").as("movimento")
) ++ colunasVp.map { nome =>
   (floor(rand() * 93) + 5).cast("int").cast("string").as(nome)
}

val df = spark.range(1000).select(expressoes: _*)


val df2 = df.select(
  col("chave"),
  col("movimento"),
  (floor(rand() * 93) + 5).cast("int").cast("string").as("vp4"),
  (floor(rand() * 93) + 5).cast("int").cast("string").as("vp5"),
  (floor(rand() * 93) + 5).cast("int").cast("string").as("vp6"),
  (floor(rand() * 93) + 5).cast("int").cast("string").as("vp7")
)



df.write.mode("overwrite").parquet("/tmp/vps/df1")
df2.write.mode("overwrite").parquet("/tmp/vps/df2")
