import com.treasuredata.spark._
val td = spark.td
val d = td.df("sample_datasets.www_access")
d.show
val errors = d.filter("code != 200")
errors.write.csv("${test_s3_folder}/result/")
