

# Run app

--------------

一段目
```
# hadoop jar CalcRegionAggregation.jar sample.mr.JoinSalesDataJob /data/sales_sample/sales /data/sales_sample/sales_detail /output/job1
```

二段目
```
# hadoop jar CalcRegionAggregation.jar sample.mr.JoinRegionAndItemJob /output/job1 /output/job2 /data/sales_sample/shoplist/shoplist.tsv /data/sales_sample/itemlist/itemlist.tsv
```

